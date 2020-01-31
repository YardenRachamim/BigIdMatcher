package bigId.matcher;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

/***
 * This class is responsible to give simple matching service to a certain client.
 * The client provide a valid URL to read input from, set of string to search for and a valid output file path.
 * The class output is:
 *      for each chunk in length of 1000 lines and for each string in the set:
 *          <Name> --> [[lineOffset=<Match_1 relative chink offset>, charOffset=<Match_1 relative char in chunk offset]>*]
 *
 * For example for the url: http://norvig.com/big.txt and for the String 'Timothy', the output will be:
 *      Timothy --> [[lineOffset=13000, charOffset=19775],[lineOffset=13000, charOffset=42023]]
 */
public class MatchFinderManager {
    //region CONSTANTS
    static final String POISON_PILL = "poisonPill";  // Poison pill unique key, using to kill aggregator thread
    private static final String REQUEST_METHOD = "GET";
    private static final int CONNECTION_TIMEOUT = 30 * 1000;  // In MS
    private static final int READ_TIMEOUT = 10 * 1000;  // In MS
    private static final int CHUNK_SIZE = 1000;  // How many lines to read for each search chunk
    //endregion CONSTANTS

    //region Fields
    private final Set<String> fTextToFindSet;
    private final BlockingQueue<Map<String, List<MatchInLineLocation>>> fMatchingPairsToAggregate;
    private String fTextToReadUrl;
    private ExecutorService fMatchSearcherExecutor;
    private HttpURLConnection fUrlConnection;
    private Thread fAggregator;
    private String fOutputFilePath;
    //endregion Fields

    //region Constructor
    public MatchFinderManager(Set<String> textToFindSet, String textToReadUrl, String outputFilePath) {
        fTextToFindSet = textToFindSet;
        fTextToReadUrl = textToReadUrl;
        fMatchingPairsToAggregate = new LinkedBlockingDeque<>();
        fOutputFilePath = outputFilePath;
        initializeMatchSearcherExecutor();
    }

    private void initializeMatchSearcherExecutor() {
        int numOfCores = Runtime.getRuntime().availableProcessors() - 1;  // -1 because of the Aggregator thread

        numOfCores = Math.max(numOfCores, 1);
        fMatchSearcherExecutor = Executors.newFixedThreadPool(numOfCores);
    }
    //endregion Constructor

    public void run() {
        searchAndAggregate();
    }

    /***
     * This method responsible to commit all the action that necessary
     * in order to make this class work correctly
     * First initializing Aggregator thread of type ChunksAggregator
     * Second open a http connection and read the input 1000 lines at a time
     * for each chunk of lines create a task of type MatchSearcher
     * and wait all the tasks to end (Including the aggregation task)
     */
    private void searchAndAggregate() {
        try {
            initializeAggregator();
            openUrlConnection();
            startAllSearchingTasks();
            joinAllSearchingTasks();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
            System.exit(1);
        } finally {
            terminateAggregator();
            disconnectFromUrlConnection();
        }
    }

    //region MatchSearching Utils
    /***
     * Read chunks of data from the HTTP connection (1000 lines)
     * and create new MatchSearcher task
     * @throws IOException in case of failure in getting the url InputStream
     */
    private void startAllSearchingTasks() throws IOException {
        // read the output from the server
        InputStream inputStream = getConnectionInputStream();
        String line;
        long currentLineOffset = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        ArrayList<String> textChunk = new ArrayList<>(CHUNK_SIZE);

        while ((line = reader.readLine()) != null)
        {
            textChunk.add(line);

            boolean isChunkSizeReached = textChunk.size() == CHUNK_SIZE;
            if(isChunkSizeReached){
                createSingleSearchingTask(textChunk, currentLineOffset);
                // Update to the next task params
                currentLineOffset += CHUNK_SIZE;
                textChunk = new ArrayList<>(CHUNK_SIZE);
            }
        }

        boolean isAnotherTaskNeeded = textChunk.size() > 0;
        if(isAnotherTaskNeeded){
            createSingleSearchingTask(textChunk, currentLineOffset);
        }
    }

    /***
     * Wait till all MatchSearcher tasks will terminate.
     * @throws InterruptedException In case of operating-system or client interrupt
     */
    private void joinAllSearchingTasks() throws InterruptedException {
        fMatchSearcherExecutor.shutdown();
        fMatchSearcherExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MICROSECONDS);
    }

    /***
     * Create single MatchSearcher task
     * and send an execution task to the Executor
     * @param textChunk All lines to search in
     * @param currentLineOffset Relative line offset of all input text
     */
    private void createSingleSearchingTask(ArrayList<String> textChunk, long currentLineOffset) {
        MatchSearcher matcher = new MatchSearcher(fMatchingPairsToAggregate,
                textChunk,
                fTextToFindSet,
                currentLineOffset);

        fMatchSearcherExecutor.execute(matcher);
    }
    //endregion MatchSearching Utils

    //region Aggregator Utils

    /***
     * Initialization of the ChunksAggregator thread
     */
    private void initializeAggregator() {
        fAggregator = new Thread(new ChunksAggregator(fMatchingPairsToAggregate, fOutputFilePath));

        fAggregator.start();
    }

    /***
     * This method responsible on inserting a poison pill to the Blocking queue.
     * by doing so it announced to the ChunksAggregator consumer that there are no more producers
     */
    private void insertPoisonPillToAggregator() {
        HashMap<String, List<MatchInLineLocation>> poisonPill =  new HashMap<>();

        poisonPill.put(POISON_PILL, null);
        fMatchingPairsToAggregate.add(poisonPill);
    }

    /***
     * Wait till ChungAggregator producer thread will finnish is task
     * e.g. wait for the poison pill to be consumed by him
     * @throws InterruptedException In case of operating-system or client interrupt
     */
    private void joinAggregator() throws InterruptedException {
        fAggregator.join();
    }

    /***
     * Define the order of task that will lead to the ChungAggregator termination
     * first insert poison pill to the blocking queue
     * Second wait till ChungAggregator will consume it
     */
    private void terminateAggregator() {
        insertPoisonPillToAggregator();

        try {
            joinAggregator();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    //endregion Aggregator Utils

    //region HttpUrlConnection utils

    /***
     * Define the params of the http connection, and perform connection.
     * @throws IOException in case of failure when setting the params or in the connection opening
     */
    private void openUrlConnection() throws IOException {
        URL source = new URL(fTextToReadUrl);
        fUrlConnection = (HttpURLConnection) source.openConnection();
        setConnectionParams();
        fUrlConnection.connect();
    }

    /***
     * Set the http request params using the constant params:
     * REQUEST_METHOD, CONNECTION_TIMEOUT, READ_TIMEOUT
     * @throws IOException in case of failure when setting the params
     */
    private void setConnectionParams() throws IOException {
        fUrlConnection.setRequestMethod(REQUEST_METHOD);
        fUrlConnection.setConnectTimeout(CONNECTION_TIMEOUT);
        fUrlConnection.setReadTimeout(READ_TIMEOUT);
    }

    /***
     * Get the actual input stream of the http connection
     * @return InputStream og the fUrlConnection connection
     * @throws IOException in case of failure on getting the input stream
     */
    private InputStream getConnectionInputStream() throws IOException {
        InputStream inputStream = fUrlConnection.getInputStream();

        if(inputStream == null){
            throw new IOException();
        }

        return inputStream;
    }

    /***
     * Terminate the http connection when all task are done, or in case of a failure
     */
    private void disconnectFromUrlConnection() {
        fUrlConnection.disconnect();
    }
    //endregion HttpUrlConnection utils
}
