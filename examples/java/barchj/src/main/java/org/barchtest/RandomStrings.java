package org.barchtest;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomStrings {

    // where not making numbers for this generator
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final SecureRandom random = new SecureRandom();

    /**
     * Generates a single random string of a specified length.
     * 
     * @param length The length of the desired random string.
     * @return A randomly generated alphanumeric string.
     */
    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            // Pick a random character from the CHARACTERS pool
            int randomIndex = random.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

    /**
     * Generates a list of n random strings, each of length k.
     *
     * @param n The number of strings to generate.
     * @param k The length of each string.
     * @return A List of the generated random strings.
     */
    public static String[] generateStrings(int n, int k) {
        String[] randomStrings = new String[n];
        for (int i = 0; i < n; i++) {
            randomStrings[i] = generateRandomString(k);
        }
        return randomStrings;
    }

}
