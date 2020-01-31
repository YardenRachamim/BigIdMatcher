package big.id.matcher;

/***
 * This class represent a Single match data.
 */
class MatchInLineLocation {
    private long fLineOffset;
    private long fCharOffset;

    public MatchInLineLocation(long lineOffSet, long charOffSet){
        fLineOffset = lineOffSet;
        fCharOffset = charOffSet;
    }

    public String toString(){
        String inLineLocation = String.format("[lineOffset=%d, charOffset=%d]", fLineOffset, fCharOffset);

        return inLineLocation;
    }
}
