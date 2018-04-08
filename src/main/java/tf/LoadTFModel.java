package tf;

import org.tensorflow.*;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * @author apollo
 * @date 17-9-3
 * https://stackoverflow.com/documentation/tensorflow/10718/save-tensorflow-model-in-python-and-load-with-java#t=201709030336395954421
 */
public class LoadTFModel {

    public static void main(String[] args) throws IOException {
        // good idea to print the version number, 1.2.0 as of this writing
        System.out.println(TensorFlow.version());

        /* load the model Bundle */
        SavedModelBundle b = SavedModelBundle.load("tmp/model", "serve");

//        for (Iterator<Operation> it = b.graph().operations(); it.hasNext(); ) {
//            Operation operation = it.next();
//
//            System.out.println(operation.name());
//        }


        Random random = new Random();
        int[] xValue = new int[600];
        for (int i = 0; i < 600; i++) {
            xValue[i] = random.nextInt(5000);
        }
        float[] yValue = new float[10];
        for (int i = 0; i < 10; i++) {
            yValue[i] = 0;
        }


        // create the session from the Bundle
        Session sess = b.session();
        // create an input Tensor, value = 2.0f
        Tensor x = Tensor.create(
                new long[]{1, 600},
                IntBuffer.wrap(xValue)
        );
        Tensor y = Tensor.create(
                new long[]{1, 10},
                FloatBuffer.wrap(yValue)
        );
        Tensor keepProb = Tensor.create(
                new long[]{1},
                FloatBuffer.wrap(new float[]{1})
        );




        // run the model and get the result, 4.0f.
        long[] prediction = sess.runner()
                .feed("input_x", x)
                .feed("input_y", y)
                .feed("keep_prob", keepProb)
                .fetch("score/ArgMax")
                .run()
                .get(0)
                .copyTo(new long[1]);

        // print out the result.
        System.out.println(prediction[0]);
    }
}