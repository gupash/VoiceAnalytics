package org.infogain.voice.customInputFormat;

import edu.cmu.sphinx.api.SpeechResult;
import edu.cmu.sphinx.api.StreamSpeechRecognizer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.infogain.voice.utils.AudioLibs;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class WavFileRecordReader extends RecordReader<DoubleWritable, Text> {


    StreamSpeechRecognizer recognizer = new AudioLibs().recognizer();
    FSDataInputStream fileIn = null;
    private double length;
    private DoubleWritable key = null;
    private Text value = null;
    private String convertedText = "";
    public WavFileRecordReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        FileSplit split = ((FileSplit) inputSplit);
        org.apache.hadoop.conf.Configuration conf = taskAttemptContext.getConfiguration();
        final Path file = split.getPath();

        FileSystem fs = file.getFileSystem(conf);
        fileIn = fs.open(file);

        InputStream is = fileIn.getWrappedStream();
        BufferedInputStream bfs = new BufferedInputStream(is);

        AudioInputStream fileStream;

        try {
            fileStream = AudioSystem.getAudioInputStream(bfs);

            final AudioFormat format = fileStream.getFormat();

            length = (fileStream.getFrameLength() + 0.0) / format.getFrameRate();

            recognizer.startRecognition(bfs);
            SpeechResult result;
            while ((result = recognizer.getResult()) != null) {
                convertedText += String.format("Hypothesis: %s\n", result.getHypothesis());
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new DoubleWritable(length);
            value = new Text(convertedText);
            return true;
        } else return false;
    }

    @Override
    public DoubleWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        recognizer.stopRecognition();
        fileIn.close();
    }
}
