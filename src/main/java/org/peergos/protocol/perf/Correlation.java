package org.peergos.protocol.perf;

public class Correlation {

    public static int id(byte[] input) {
        if(input.length < 4) {
            throw new IllegalArgumentException("Input length must be >=4");
        }
        return (input[0] & 0xFF) | ((input[1] & 0xFF) << 8) | ((input[2] & 0xFF) << 16) | ((input[3] & 0xFF) << 24);
    }

}
