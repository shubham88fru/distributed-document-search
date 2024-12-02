package search;

import model.DocumentData;

import java.util.*;

public class TFIDF {

    /**
     * @param words
     * @param term
     * @return the frequency of term in the list of words.
     */
    public static double calculateTermFrequency(List<String> words, String term) {
        long count = 0;
        for (String word : words) {
            if (term.equalsIgnoreCase(word)) {
                count++;
            }
        }
        return (double) count / (double) words.size();
    }

    /**
     * @param words - all words in a document
     * @param terms - list of terms
     * @return DocumentData object that contains term frequencies of all terms in the document.
     */
    public static DocumentData createDocumentData(List<String> words, List<String> terms) {
        DocumentData documentData = new DocumentData();
        for (String term: terms) {
            double termFrequency = calculateTermFrequency(words, term);
            documentData.putTermFrequency(term, termFrequency);
        }

        return documentData;
    }

    /**
     * @param term
     * @param documentResults
     * @return the IDF of a term.
     */
    private static double getInverseDocumentFrequency(String term, Map<String, DocumentData> documentResults) {
        double nt = 0.0;
        for (String document: documentResults.keySet()) {
            DocumentData documentData = documentResults.get(document);
            double termFrequency = documentData.getTermFrequency(term);
            if (termFrequency > 0.0) { //the document contains current term.
                nt +=1;
            }
        }

        return nt == 0.0 ? 0.0: Math.log10(documentResults.size() / nt);
    }

    /**
     * @param terms
     * @param documentResults - Map that maps each document to its document data.
     * @return the IDF for every term.
     */
    private static Map<String, Double> getTermToInverseDocumentFrequency(List<String> terms,
                                                                         Map<String, DocumentData> documentResults) {
        Map<String, Double> termToIDF = new HashMap<>();
        for (String term: terms) {
            double idf = getInverseDocumentFrequency(term, documentResults);
            termToIDF.put(term, idf);
        }
        return termToIDF;
    }


    private static double calculateDocumentScore(List<String> terms,
                                                 DocumentData documentData,
                                                 Map<String, Double> termToInverseDocumentFrequency) {
        double score = 0.0; //score of a document
        for (String term: terms) {
            double termFrequency = termToInverseDocumentFrequency.get(term);
            double idf = documentData.getTermFrequency(term);
            score += idf * termFrequency;
        }

        return score;
    }


    public static Map<Double, List<String>> getDocumentsSortedByScore(List<String> terms,
                                                                      Map<String, DocumentData> documentResults) {
        TreeMap<Double, List<String>> scoreToDocuments = new TreeMap<>(); //Treemap so that the map is always sorted by score.

        Map<String, Double> termToInverseDocumentFrequency = getTermToInverseDocumentFrequency(terms, documentResults);
        for (String document: documentResults.keySet()) {
            DocumentData documentData = documentResults.get(document);
            double score = calculateDocumentScore(terms, documentData, termToInverseDocumentFrequency);

            addDocumentScoreToMap(scoreToDocuments, score, document);
        }
        return scoreToDocuments.descendingMap();
    }

    private static void addDocumentScoreToMap(TreeMap<Double, List<String>> scoreToDoc, double score, String document) {
        List<String> documentsWithCurrentScore = scoreToDoc.get(score);
        if (documentsWithCurrentScore == null) {
            documentsWithCurrentScore = new ArrayList<>();
        }

        documentsWithCurrentScore.add(document);
        scoreToDoc.put(score, documentsWithCurrentScore);
    }

    public static List<String> getWordsFromLine(String line) {
        return Arrays.asList(line.split("(\\.)+|(,)+|( )+|(-)+|(\\?)+|(!)+|(;)+|(:)+|(/d)+|(/n)+"));
    }

    public static List<String> getWordsFromLines(List<String> lines) {
        List<String> words = new ArrayList<>();
        for (String line : lines) {
            words.addAll(getWordsFromLine(line));
        }

        return words;
    }

}