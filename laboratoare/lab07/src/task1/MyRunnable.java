package task1;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

public class MyRunnable implements Runnable{
    ArrayList<Integer> partialPath;
    int destination;
    int[][] graph;
    ExecutorService es;

    public MyRunnable(ArrayList<Integer> partialPath, int destination, int[][] graph, ExecutorService es) {
        this.partialPath = new ArrayList<>();
        this.partialPath.addAll(partialPath);
        this.destination = destination;
        this.graph = graph;
        this.es = es;
    }

    @Override
    public void run() {
        if (partialPath.get(partialPath.size() - 1) == destination) {
            System.out.println(partialPath);
            es.shutdown();
            return;
        }

        // se verifica nodurile pentru a evita ciclarea in graf
        int lastNodeInPath = partialPath.get(partialPath.size() - 1);
        for (int[] ints : graph) {
            if (ints[0] == lastNodeInPath) {
                if (partialPath.contains(ints[1]))
                    continue;
                ArrayList<Integer> newPartialPath = new ArrayList<>(partialPath);
                newPartialPath.add(ints[1]);
                es.submit(new MyRunnable(newPartialPath, destination, graph, es));
            }
        }
    }
}
