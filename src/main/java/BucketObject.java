public class BucketObject implements Comparable<BucketObject> {

    private String hashtag;
    private int frequency;
    private int delta;

    public BucketObject(String hashtag, int frequency, int delta) {
        this.hashtag = hashtag;
        this.frequency = frequency;
        this.delta = delta;
    }

    @Override
    public int compareTo(BucketObject o) {
        long difference = this.frequency - o.getFrequency();
        if (difference < 0) {
            return 1;
        } else if (difference > 0) {
            return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "BucketObject{" +
                "hashtag='" + hashtag + '\'' +
                ", frequency=" + frequency +
                ", delta=" + delta +
                '}';
    }

    public String getHashtag() {
        return this.hashtag;
    }

    public void incrementFrequency() {
        this.frequency++;
    }

    public int getFrequency() {
        return this.frequency;
    }

    public int getDelta() {
        return this.delta;
    }

}
