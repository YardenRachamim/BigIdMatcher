package big.id.matcher;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/***
 * This class responsible to get Matching pairs from different chunks
 * and aggregate them together in order to create full matching pairs
 * this class is also responsible to provide the final output of the Matcher
 */
class ChunksAggregator implements Runnable{
    //region Fields
    private final BlockingQueue<Map<String, List<MatchInLineLocation>>> fMatchingPairsToAggregate;
    private Map<String, List<MatchInLineLocation>> fAllPairs;
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
            }
        }
    }

    private void accumulatePairsChunk(Map<String, List<MatchInLineLocation>> singlePairChunk) {
        for (Map.Entry<String, List<MatchInLineLocation>> singlePair : singlePairChunk.entrySet()) {
            String key = singlePair.getKey();
            List<MatchInLineLocation> value = singlePair.getValue();

            accumulateSinglePair(key, value);
        }
    }

    private void accumulateSinglePair(String key, List<MatchInLineLocation> value) {
        List<MatchInLineLocation> listToModify = fAllPairs.getOrDefault(key, new ArrayList<>());

        listToModify.addAll(value);
        fAllPairs.put(key, listToModify);
    }
    //endregion Aggregation action

    //region Aggregation results
    private void writeResults() {
        try {
            String results = getResults();
            writeResultsToStdIn(results);
            writeResultsToOutputFile(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getResults() {
        StringBuilder output = new StringBuilder();

        for(Map.Entry<String, List<MatchInLineLocation>> singlePair : fAllPairs.entrySet()){
            String key = singlePair.getKey();
            List<MatchInLineLocation> value = singlePair.getValue();

            output.append(key).append(" --> ").append("[");

            for(MatchInLineLocation item : value){
                output.append(item).append(",");
            }

            output.replace(output.length() - 1, output.length(), "");  // Remove last ','
            output.append("]").append('\n');
        }

        return output.toString();
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
