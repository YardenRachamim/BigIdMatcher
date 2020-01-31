package bigId.matcher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

/***
 * This class responsible to get Matching pairs from different chunks
 * and aggregate them together in order to create full matching pairs
 * this class is also responsible to provide the final output of the Matcher
 */
class ChunksAggregator implements Runnable{
    //region Fields
    private final BlockingQueue<Map<String, List<MatchInLineLocation>>> fMatchingPairsToAggregate;
    private Map<String, SortedSet<MatchInLineLocation>> fAllPairs;
    private String fOutputFilePath;
    //endregion Fields

    //region Constructor
    public ChunksAggregator(BlockingQueue<Map<String, List<MatchInLineLocation>>> matchingPairsToAggregate,
                            String outputFilePath){
        fMatchingPairsToAggregate = matchingPairsToAggregate;
        fAllPairs = new HashMap<>();
        fOutputFilePath = outputFilePath;
    }
    //endregion Constructor

    @Override
    public void run() {
        aggregateAllPairs();
        writeResults();
    }

    //region Aggregation action
    /***
     * While there are more producers, the aggregator is waiting for more input to come.
     * when a input is entering the queue this method will start the process of accumulate
     * all the pairs to a single pair mapping by keeping the Set of Match data sorted
     * It will stop waiting to the producers when getting the poison pill.
     */
    private void aggregateAllPairs() {
        boolean isThereMoreProducers = true;
        while(isThereMoreProducers) {
            try {
                Map<String, List<MatchInLineLocation>> singlePairChunk = fMatchingPairsToAggregate.take();

                if(singlePairChunk.containsKey(MatchFinderManager.POISON_PILL)){
                    isThereMoreProducers = false;
                }else {
                    accumulatePairsChunk(singlePairChunk);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }

    /***
     * This method iterate over a lines chunk and accumulate it to the final pair mapping
     * doing so by keeping the match data sorted
     * @param singlePairChunk single mapping between text to find and a list of matching data
     */
    private void accumulatePairsChunk(Map<String, List<MatchInLineLocation>> singlePairChunk) {
        for (Map.Entry<String, List<MatchInLineLocation>> singlePair : singlePairChunk.entrySet()) {
            String key = singlePair.getKey();
            List<MatchInLineLocation> value = singlePair.getValue();

            accumulateSinglePair(key, value);
        }
    }

    /***
     * This method perform the actual accumulation between tha pairs of the text to find that
     * already known to the ChunkAggregator and the pairs that are in the current chunk
     * it doing so while maintaining the match data sorted
     * @param key String that we find a match to
     * @param value list of all matches to that string in the current chunk
     */
    private void accumulateSinglePair(String key, List<MatchInLineLocation> value) {
        SortedSet<MatchInLineLocation> listToModify = fAllPairs.getOrDefault(key, new TreeSet<>());

        listToModify.addAll(value);
        fAllPairs.put(key, listToModify);
    }
    //endregion Aggregation action

    //region Aggregation results
    /***
     * This class takes all the pairs after aggregation and formatting it
     * to a specific string result
     * @return String of formatted  results
     */
    private String getResults() {
        StringBuilder output = new StringBuilder();

        for(Map.Entry<String, SortedSet<MatchInLineLocation>> singlePair : fAllPairs.entrySet()){
            String key = singlePair.getKey();
            SortedSet<MatchInLineLocation> value = singlePair.getValue();

            output.append(key).append(" --> ").append("[");

            for(MatchInLineLocation item : value){
                output.append(item).append(",");
            }

            output.replace(output.length() - 1, output.length(), "");  // Remove last ','
            output.append("]").append('\n');
        }

        return output.toString();
    }

    /***
     * This method responsible of writing the final results of the Matches
     * once to the StdIn and second to the give File output
     */
    private void writeResults() {
        try {
            String results = getResults();
            writeResultsToStdIn(results);
            writeResultsToOutputFile(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeResultsToStdIn(String results) {
        System.out.println(results);
    }

    private void writeResultsToOutputFile(String results) throws IOException {
        File outputFile =  getOutputFile();

        boolean isOutputFileExists = outputFile != null;
        if(isOutputFileExists) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
                fileOutputStream.write(results.getBytes());
            }
        }
    }

    private File getOutputFile() throws IOException {
        File outPutFile = new File(fOutputFilePath);

        boolean isOutputFileExists = outPutFile.exists();
        if(!isOutputFileExists){
            isOutputFileExists  = outPutFile.createNewFile();
        }

        if(!isOutputFileExists){
            outPutFile = null;
        }

        return outPutFile;
    }
    //endregion Aggregation results
}
