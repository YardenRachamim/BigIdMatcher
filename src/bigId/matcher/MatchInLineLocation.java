package bigId.matcher;

/***
 * This class represent a Single match data.
 */
class MatchInLineLocation implements Comparable<MatchInLineLocation>{
    private long fLineOffset;
    private long fCharOffset;

    private long getLineOffset() {
        return fLineOffset;
    }

    private long getCharOffset() {
        return fCharOffset;
    }

    public MatchInLineLocation(long lineOffSet, long charOffSet){
        fLineOffset = lineOffSet;
        fCharOffset = charOffSet;
    }

    @Override
    public String toString(){
        String inLineLocation = String.format("[lineOffset=%d, charOffset=%d]", fLineOffset, fCharOffset);

        return inLineLocation;
    }

    /***
     * Compare between 2 MatchInLineLocation.
     * First it compare the line off set and in case of equality
     * it will compare also the char offset
     * @param other MatchInLineLocation to compare to
     * @return int 1 - current is bigger, -1 - other is bigger, 0 - equal
     */
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
