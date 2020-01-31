package big.id.matcher;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/***
 * This class responsible of finding Mapping between String and a 1000 line text
 * Each match will be collected separately and when done the matched will be sent to an aggregator in order to reorder them
 */
class MatchSearcher implements Runnable{
    //region Fields
    private final List<String> fLinesToCheck;
    private final Set<String> fTextToFindSet;
    private final BlockingQueue<Map<String, List<MatchInLineLocation>>> fMatchingPairsToAggregate;
    private long fFirstLineOffset;
    //endregion Fields

    //region constructor
    public MatchSearcher(BlockingQueue<Map<String, List<MatchInLineLocation>>> matchingPairsToAggregate,
                         List<String> linesToCheck,
                         Set<String> textToMatchSet,
                         long firstLineOffset){
        fMatchingPairsToAggregate = matchingPairsToAggregate;
        fLinesToCheck = linesToCheck;
        fTextToFindSet = textToMatchSet;
        fFirstLineOffset = firstLineOffset;
    }
    //endregion constructor

    @Override
    public void run() {
        searchForMatches();
    }

    private void searchForMatches() {
        Map<String, List<MatchInLineLocation>> matchingPairs = new HashMap<>();
        long lineFirstCharOffset = 0;

        for(String line : fLinesToCheck) {
            getMatchMappingInSingleLine(matchingPairs, line, lineFirstCharOffset);
            lineFirstCharOffset += line.length();  // Update lineFirstCharOffset, for the next iteration
        }

        boolean isFoundMatchingPairs = matchingPairs.size() > 0;
        if(isFoundMatchingPairs) {
            fMatchingPairsToAggregate.add(matchingPairs);
        }
    }

    /***
     * This method search matches in a single line and accumulate it to matchingPairs
     * @param matchingPairs Match mapping between String and match data
     * @param line the line to search in
     * @param lineFirstCharOffset the line relative offset comparing to the all text
     */
    private void getMatchMappingInSingleLine(Map<String, List<MatchInLineLocation>> matchingPairs, String line, long lineFirstCharOffset) {
        for (String textToFind : fTextToFindSet) {
            String patternString = "\\b(" + textToFind + ")\\b";
            Pattern pattern = Pattern.compile(patternString);
            Matcher matcher = pattern.matcher(line);

            while (matcher.find()) {
                long currentMatchCharOffset = (lineFirstCharOffset + matcher.start(1));
                MatchInLineLocation matchInLineLocation = new MatchInLineLocation(fFirstLineOffset, currentMatchCharOffset);

                addSingleMatchRecord(textToFind, matchingPairs, matchInLineLocation);
            }
        }
    }

    /***
     * This method accumulate single match into the given matchingPairs map
     * @param key the text we looking for
     * @param matchingPairs Match mapping between String and match data
     * @param matchInLineLocation Match data
     */
    private void addSingleMatchRecord(String key, Map<String, List<MatchInLineLocation>> matchingPairs, MatchInLineLocation matchInLineLocation) {
        List<MatchInLineLocation> listToModify = matchingPairs.getOrDefault(key, new ArrayList<>());

        listToModify.add(matchInLineLocation);
        matchingPairs.put(key, listToModify);
    }
}
