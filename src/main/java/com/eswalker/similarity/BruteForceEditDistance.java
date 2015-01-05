package com.eswalker.similarity;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class BruteForceEditDistance {
    public static void main(String args[]) throws IOException {

        long start = System.currentTimeMillis();

        ArrayList<String> elements = new ArrayList<String>();

        if (args.length < 2)
            System.out.println("usage: com.eswalker.similarity.BruteForceEditDistance [filename] [threshold]");

        int threshold = Integer.parseInt(args[1]);

        BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
        String line;
        while ((line = br.readLine())!= null) {
            elements.add(line);
        }

        for (int i = 0; i < elements.size(); i++) {
            for (int j = i + 1; j < elements.size(); j++) {
                String a = elements.get(i);
                String b = elements.get(j);
                int edit = StringUtils.getLevenshteinDistance(a, b);
                if (edit <= threshold)
                    System.out.println(edit + "|" + i + "|" + j + "|" + a + "|" + b);
            }
            if (i % (elements.size() / 100) == 0)
                System.err.print(".");

        }

        long end = System.currentTimeMillis();
        System.err.println("\nRUNNING TIME (MS): " + (end-start));
    }
}
