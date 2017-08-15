/**
 * Created by lsx on 2017/3/23.
 */

public class ThreadTest
{
    static double[] doubles = new double[300];

    static double maxSim=0.0;

    public static void main(String[] args) throws InterruptedException
    {

        Thread t1 = new process("a", 0, 100);
        Thread t2 = new process("b", 100, 200);
        Thread t3 = new process("c", 200, 300);


        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        for (int i = 0; i < 20; i++)
        {
            System.out.println("我是main线程");
        }
    }

    static class process extends Thread
    {
        int start = 0;
        int end = 0;

        process(String s, int start, int end)
        { //给该线程取一个名字，用getName()方法可以去到该名字
            super(s);
            this.start = start;
            this.end = end;
        }

        @Override
        public void run()
        {

            for (int i = start; i < end; i++)
            {
                synchronized (doubles)
                {
                    System.out.println("我是" + getName() + "线程");
//                    doubles[i] = i;
                    if (i>maxSim){
                        maxSim=(double)i;
                    }
                }
            }
        }
    }

}

