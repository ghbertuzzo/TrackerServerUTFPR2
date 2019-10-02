package trackerserverutfpr;

public class MainProcessingModule {

    public static void main(String[] args) {
        // TODO code application logic here        
        int timeSleep = 1; //IN SECONDS
        int limitThreads = 1000;
        Thread threadProcModule = null;
        ProcessingModule procModule = new ProcessingModule(timeSleep, limitThreads);
        threadProcModule = new Thread(procModule);
        threadProcModule.start();
    }
}
