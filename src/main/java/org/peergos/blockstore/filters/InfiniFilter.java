
package org.peergos.blockstore.filters;

public class InfiniFilter extends QuotientFilter {

	long empty_fingerprint;
	FingerprintGrowthStrategy.FalsePositiveRateExpansion fprStyle = FingerprintGrowthStrategy.FalsePositiveRateExpansion.UNIFORM;

	public void set_fpr_style(FingerprintGrowthStrategy.FalsePositiveRateExpansion val) {
		fprStyle = val;
	}
	
	InfiniFilter(int power_of_two, int bits_per_entry) {
		super(power_of_two, bits_per_entry);
		max_entries_before_expansion = (long)(Math.pow(2, power_of_two_size) * expansion_threshold);
		empty_fingerprint = (1L << fingerprintLength) - 2L;
	}
	
	protected boolean compare(long index, long fingerprint) {
		long generation = parse_unary(index);
		long first_fp_bit = index * bitPerEntry + 3;
		long last_fp_bit = index * bitPerEntry + 3 + fingerprintLength - (generation + 1);
		long actual_fp_length = last_fp_bit - first_fp_bit;
		long existing_fingerprint = filter.getFromTo(first_fp_bit, last_fp_bit);
		long mask = (1L << actual_fp_length) - 1L;
		long adjusted_saught_fp = fingerprint & mask;
		return existing_fingerprint == adjusted_saught_fp;
	}
		
	// this is the newer version of parsing the unary encoding. 
	// it is done using just binary operations and no loop. 
	// however, this optimization didn't yield much performance benefit 
	long parse_unary(long slot_index) {
		long f = get_fingerprint(slot_index);
		//.out.println();
		//System.out.println(get_pretty_str(slot_index));
		//print_int_in_binary(f, 32);
		long inverted_fp = ~f;
		//print_int_in_binary(inverted_fp, 32);
		long mask = (1L << fingerprintLength) - 1;
		//print_int_in_binary(mask, 32);
		long masked = mask & inverted_fp;
		//print_int_in_binary(masked, 32);
		long highest = Long.highestOneBit(masked);
		//print_int_in_binary(highest, 32);
		long leading_zeros = Long.numberOfTrailingZeros(highest);
		//System.out.println( leading_zeros );
		long age = fingerprintLength - leading_zeros - 1;
		//System.out.println( age );
		return age;
	}
	
	public boolean rejuvenate(long key) {
		long large_hash = get_hash(key);
		long fingerprint = gen_fingerprint(large_hash);
		long ideal_index = get_slot_index(large_hash);
		
		boolean does_run_exist = is_occupied(ideal_index);
		if (!does_run_exist) {
			return false;
		}
		
		long run_start_index = find_run_start(ideal_index);
		long smallest_index = find_largest_matching_fingerprint_in_run(run_start_index, fingerprint);
		if (smallest_index == -1) {
			return false;
		}
		swap_fingerprints(smallest_index, fingerprint);
		return true; 
	}

	
	long decide_which_fingerprint_to_delete(long index, long fingerprint) {
		return find_largest_matching_fingerprint_in_run(index, fingerprint);
	}
	
	// returns the index of the entry if found, -1 otherwise
	long find_largest_matching_fingerprint_in_run(long index, long fingerprint) {
		assert(!is_continuation(index));
		long matching_fingerprint_index = -1;
		long lowest_age = Integer.MAX_VALUE;
		do {
			if (compare(index, fingerprint)) {
				//System.out.println("found matching FP at index " + index);
				long age = parse_unary(index);
				if (age < lowest_age) {
					lowest_age = age;
					matching_fingerprint_index = index;
				}
			}
			index++;
		} while (is_continuation(index));
		return matching_fingerprint_index; 
	}
	
	long gen_fingerprint(long large_hash) {
		long fingerprint_mask = (1L << fingerprintLength) - 1L;
		fingerprint_mask = fingerprint_mask << power_of_two_size;
		long fingerprint = (large_hash & fingerprint_mask) >> power_of_two_size;
		long unary_mask = ~(1L << (fingerprintLength - 1L));
		long updated_fingerprint = fingerprint & unary_mask;
		/*System.out.println(); 
		print_long_in_binary(unary_mask, fingerprintLength);
		print_long_in_binary( fingerprint, fingerprintLength);
		print_long_in_binary( updated_fingerprint, fingerprintLength);*/
		return updated_fingerprint;
	}
	
	void handle_empty_fingerprint(long bucket_index, QuotientFilter insertee) {
		long bucket1 = bucket_index;
		long bucket_mask = 1L << power_of_two_size; 		// setting this bit to the proper offset of the slot address field
		long bucket2 = bucket1 | bucket_mask;	// adding the pivot bit to the slot address field
		insertee.insert(empty_fingerprint, bucket1, false);
		insertee.insert(empty_fingerprint, bucket2, false);
	}
	
	private static int prep_unary_mask(int prev_FP_size, int new_FP_size) {
		int fingerprint_diff = new_FP_size - prev_FP_size;
		
		int unary_mask = 0;
		for (int i = 0; i < fingerprint_diff + 1; i++) {
			unary_mask <<= 1;
			unary_mask |= 1;
		}
		unary_mask <<= new_FP_size - 1 - fingerprint_diff;
		return unary_mask;
	}
	
	void expand() {
		int new_fingerprint_size = FingerprintGrowthStrategy.get_new_fingerprint_size(original_fingerprint_size, num_expansions, fprStyle);
		//System.out.println("FP size: " + new_fingerprint_size);
		QuotientFilter new_qf = new QuotientFilter(power_of_two_size + 1, new_fingerprint_size + 3);
		Iterator it = new Iterator(this);		
		long unary_mask = prep_unary_mask(fingerprintLength, new_fingerprint_size);
		
		long current_empty_fingerprint = empty_fingerprint;
		empty_fingerprint = (1L << new_fingerprint_size) - 2; // One 
				
		while (it.next()) {
			long bucket = it.bucket_index;
			long fingerprint = it.fingerprint;
			if (it.fingerprint != current_empty_fingerprint) {
				long pivot_bit = (1 & fingerprint);	// getting the bit of the fingerprint we'll be sacrificing 
				long bucket_mask = pivot_bit << power_of_two_size; // setting this bit to the proper offset of the slot address field
				long updated_bucket = bucket | bucket_mask;	 // adding the pivot bit to the slot address field
				long chopped_fingerprint = fingerprint >> 1; // getting rid of this pivot bit from the fingerprint 
				long updated_fingerprint = chopped_fingerprint | unary_mask;				
				new_qf.insert(updated_fingerprint, updated_bucket, false);
				
				/*System.out.println(bucket); 
				System.out.print("bucket1      : ");
				print_int_in_binary( bucket, power_of_two_size);
				System.out.print("fingerprint1 : ");
				print_int_in_binary((int) fingerprint, fingerprintLength);
				System.out.print("pivot        : ");
				print_int_in_binary((int) pivot_bit, 1);
				System.out.print("mask        : ");
				print_int_in_binary((int) unary_mask, new_fingerprint_size);
				System.out.print("bucket2      : ");
				print_int_in_binary((int) updated_bucket, power_of_two_size + 1);
				System.out.print("fingerprint2 : ");
				print_int_in_binary((int) updated_fingerprint, new_fingerprint_size);
				System.out.println();
				System.out.println();*/
			}
			else {
				handle_empty_fingerprint(it.bucket_index, new_qf);
			}
		}
		
		empty_fingerprint = (1L << new_fingerprint_size) - 2 ;
		fingerprintLength = new_fingerprint_size;
		bitPerEntry = new_fingerprint_size + 3;
		filter = new_qf.filter;
		power_of_two_size++;
		num_extension_slots += 2;
		max_entries_before_expansion = (int)(Math.pow(2, power_of_two_size) * expansion_threshold);
	}
	
	/*public void print_filter_summary() {	
		super.print_filter_summary();
		
		TreeMap<Integer, Integer> histogram = new TreeMap<Integer, Integer>();
		
		for (int i = 0; i <= fingerprintLength; i++) {
			histogram.put(i, 0);
		}
		
		
		for (int i = 0; i < get_logical_num_slots_plus_extensions(); i++) {
			int age = parse_unary(i); 
			int count = histogram.get(age);
			histogram.put(age, count + 1);
		}
		
		System.out.println("fingerprint sizes histogram");
		System.out.println("\tFP size" + "\t" + "count");
		for ( Entry<Integer, Integer> e : histogram.entrySet() ) {
			int fingerprint_size = fingerprintLength - e.getKey() - 1;
			if (fingerprint_size >= 0) {
				System.out.println("\t" + fingerprint_size + "\t" + e.getValue());
			}
		}
		
	}*/

}
