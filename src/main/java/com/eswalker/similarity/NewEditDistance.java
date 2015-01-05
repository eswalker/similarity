package com.eswalker.similarity;

import java.io.IOException;
import java.util.HashSet;

public class NewEditDistance {

    private HashSet<Long> buckets = new HashSet<Long>();
    private int lowAscii;
    private int highAscii;

    public NewEditDistance(char lowestLetterInAlphabet,
                           char highestLetterInAlphabet) {
        this.lowAscii = (int) lowestLetterInAlphabet;
        this.highAscii = (int) highestLetterInAlphabet;
    }


    public static void main(String args[]) throws IOException {

        NewEditDistance ned = new NewEditDistance('1', '2');
        ned.addBuckets("12", 4);
        for(Long l: ned.buckets) {
            System.out.println(ned.hashToString(l));
        }
    }

    private String hashToString(long hash) {
        String s = "";
        int ascii = highAscii;
        while (hash != 0) {
            long count = hash % 31;
            for (int i = 0; i < count; i++)
                s += "" + (char) ascii;
            ascii--;
            hash /= 31;

        }
        if (s.isEmpty())
            return "EMPTY";
        return s;
    }



    public HashSet<Long> getBuckets() { return buckets; }

    public void addBuckets(String original, int threshold) {


        // step 1: to character count array
        int alphabetSize = highAscii - lowAscii + 1;
        int[] counts = new int[alphabetSize];

        for (int i = 0; i < original.length(); i++) {
            int index = (int) (original.charAt(i)) - lowAscii;
            if (index >= 0 && index < counts.length) // ignore chars not in the alphabet
                counts[index]++;
        }
        addBuckets(counts, threshold);

    }

    private void addBuckets(int[] counts, int threshold) {

        buckets.add(hash(counts));
        if (threshold <= 0)
            return;

        // step 2: perform edit operations -- insert, delete, substitute

        // insertion
        for (int i = 0; i < counts.length; i++) {
            counts[i]++;
            addBuckets(counts, threshold - 2);
            counts[i]--;
        }

        // deletion
        for (int i = 0; i < counts.length; i++) {
            counts[i]--;
            if (counts[i] >= 0) {
                addBuckets(counts, threshold - 2);
            }
            counts[i]++;
        }

        // substitution
        for (int i = 0; i < counts.length; i++) {
            for (int j = 0; j < counts.length; j++) {
                if (i == j)
                    continue;
                counts[i]--;
                counts[j]++;
                if (counts[i] >= 0) {
                    addBuckets(counts, threshold - 2);
                }
                counts[j]--;
                counts[i]++;

            }
        }


    }

    private long hash(int[] counts) {
        // step 3: Hash the character count arrays

        long sum = 0;
        for (int i = 0; i < counts.length; i++)
            sum = sum * 31 + counts[i];
        return sum;
    }

    public void clearBuckets() { buckets.clear(); }

}
