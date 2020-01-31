import bigId.matcher.MatchFinderManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/***
 * Demo run of big.id.matcher
 */
public class BigIdMatcher {
    //region main arguments
    private static String urlToRead = "http://norvig.com/big.txt";
    private static String textToFindFilePath = "50 most common English first names.txt";
    private static String outputFilePath =  System.getProperty("user.dir") + "\\output.txt";
    //endregion main arguments

    public static void main(String[] args){
        Set<String> textToFindSet = initializeTextToMatchSet(textToFindFilePath);
        MatchFinderManager matchFinderManager = new MatchFinderManager(textToFindSet, urlToRead, outputFilePath);

        matchFinderManager.run();
    }

    private static Set<String> initializeTextToMatchSet(String textToMatchPath){
        File textToMatchFile = new File(textToMatchPath);
        HashSet<String> textToMatchSet = new HashSet<>();

        try (Scanner scanner = new Scanner(textToMatchFile);){
            scanner.useDelimiter("\n");

            while(scanner.hasNext()){
                String txt = scanner.next().trim();

                textToMatchSet.add(txt);
            }
        } catch (FileNotFoundException e) {
            System.err.println("problem in scanning the text to match file");
            e.printStackTrace();
        }

        return textToMatchSet;
    }
}
