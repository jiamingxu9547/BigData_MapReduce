
package wordFrequency;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordWritable implements Writable, WritableComparable<WordWritable> {

    private Text currentWord;
    private Text nextWord;

    public WordWritable(Text currentWord, Text nextWord) {
        this.currentWord = currentWord;
        this.nextWord = nextWord;
    }

    public WordWritable(String currentWord, String nextWord) {
        this(new Text(currentWord), new Text(nextWord));
    }

    public WordWritable() {
        this.currentWord = new Text();
        this.nextWord = new Text();
    }

    public void setCurrentWord(String currentWord){
        this.currentWord.set(currentWord);
    }
    public void setNextWord(String nextWord){
        this.nextWord.set(nextWord);
    }

    public Text getCurrentWord() {
        return currentWord;
    }
    
    public Text getNextWord() {
        return nextWord;
    }
    
    @Override
    public int compareTo(WordWritable otherWord) {
        int returnVal = this.currentWord.compareTo(otherWord.getCurrentWord());
        
        if(returnVal != 0){
            return returnVal;
        }
        
        if(this.nextWord.toString().equals("*")){
            return -1;
        }
        
        if(otherWord.getNextWord().toString().equals("*")){
            return 1;
        }
        
        return this.nextWord.compareTo(otherWord.getNextWord());
    }

    public static WordWritable read(DataInput in) throws IOException {
        WordWritable wordPair = new WordWritable();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        currentWord.write(out);
        nextWord.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        currentWord.readFields(in);
        nextWord.readFields(in);
    }

    @Override
    public String toString() {
          return currentWord + " " + nextWord; 
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordWritable wordPair = (WordWritable) o;

        if (nextWord != null ? !nextWord.equals(wordPair.nextWord) : wordPair.nextWord != null) return false;
        if (currentWord != null ? !currentWord.equals(wordPair.currentWord) : wordPair.currentWord != null) return false;
        
        return true;
    }

    @Override
    public int hashCode() {
        int result = currentWord != null ? currentWord.hashCode() : 0;
        result = 163 * result + (nextWord != null ? nextWord.hashCode() : 0);
        return result;
    }

    
}
