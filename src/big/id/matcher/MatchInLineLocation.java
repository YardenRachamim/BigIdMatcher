package big.id.matcher;

/***
 * This class represent a Single match data.
 */
class MatchInLineLocation implements Comparable<MatchInLineLocation>{
    private long fLineOffset;
    private long fCharOffset;

    long getLineOffset() {
        return fLineOffset;
    }

    long getCharOffset() {
        return fCharOffset;
    }

    public MatchInLineLocation(long lineOffSet, long charOffSet){
        fLineOffset = lineOffSet;
        fCharOffset = charOffSet;
    }

    public String toString(){
        String inLineLocation = String.format("[lineOffset=%d, charOffset=%d]", fLineOffset, fCharOffset);

        return inLineLocation;
    }

    @Override
    public int compareTo(MatchInLineLocation other) {
        Long currentLineOffset = fLineOffset;
        Long otherLineOffset =  other.getLineOffset();
        int compareResult = currentLineOffset.compareTo(otherLineOffset);

        if(compareResult == 0){
            Long currentCharOffset = fCharOffset;
            Long otherCharOffset =  other.getCharOffset();

            compareResult= currentCharOffset.compareTo(otherCharOffset);
        }

        return compareResult;
    }
}
