package model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DocumentData implements Serializable {
    //term v/s its frequency in the current doc.
    private Map<String, Double> termToFrequency = new HashMap<>();

    public void putTermFrequency(String term, double frequency) {
        termToFrequency.put(term, frequency);
    }

    public double getTermFrequency(String term) {
        return termToFrequency.getOrDefault(term, 0.0);
    }
}