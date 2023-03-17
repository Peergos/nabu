package org.peergos.blockstore.filters;


public class FingerprintGrowthStrategy {

	public enum FalsePositiveRateExpansion {
		UNIFORM,
		POLYNOMIAL,
		GEOMETRIC,
	}
	
	static int  get_new_fingerprint_size(int original_fingerprint_size, int num_expansions, FalsePositiveRateExpansion fprStyle) {
		
		double original_FPR = Math.pow(2, -original_fingerprint_size);
		double new_filter_FPR = 0;
		if (fprStyle == FalsePositiveRateExpansion.GEOMETRIC) {
			double factor = 1.0 / Math.pow(2, num_expansions);
			new_filter_FPR = factor * original_FPR; 
		}
		else if (fprStyle == FalsePositiveRateExpansion.POLYNOMIAL) {
			double factor = 1.0 / Math.pow(num_expansions + 1, 2);
			new_filter_FPR = factor * original_FPR; 
		}
		else if (fprStyle == FalsePositiveRateExpansion.UNIFORM) {
			new_filter_FPR = original_FPR; 
		}
		double fingerprint_size = -Math.ceil( Math.log(new_filter_FPR) / Math.log(2) );
		int fingerprint_size_int = (int) fingerprint_size;
		return fingerprint_size_int;
	}
	
	
}
