package trackerserverutfpr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProcessingModule implements Runnable {

    private int timeSleep;
    private int limitThreads;

    public ProcessingModule(int time, int limitThreads) {
        this.timeSleep = time;
        this.limitThreads = limitThreads;
    }

    @Override
    public void run() {
        while (true) {            
            //SELECT PARA PEGAR TODAS MSG NAO PROCESSSADAS
            ArrayList<TrackerST300> list = getMsgsInDB();
            
            if(!list.isEmpty()){
            
                //ALOCA PACKET DE MSGS A SEREM PROCESSADAS
                ArrayList<TrackerST300> lista = new ArrayList<>();
                for(TrackerST300 track: list){
                    lista.add(track);
                    if(lista.size()>=this.limitThreads){
                        Thread thread = null;
                        TrackerPack tp = new TrackerPack(lista);
                        thread = new Thread(tp);
                        thread.start();
                        lista = new ArrayList<>();
                    }
                }

                //SE NUMERO DE MSGS < LIMIT PROCESSA COM 1 UNICA THREADPACKET
                Thread thread = null;
                TrackerPack tp = new TrackerPack(lista);
                thread = new Thread(tp);
                thread.start();
                
            }
            
            //ESPERA N SEG PARA REPETIR O CICLO
            try {
                sleep(this.timeSleep * 1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void sleep(int n) throws InterruptedException {
        Thread.sleep(n);
    }

    private ArrayList<TrackerST300> getMsgsInDB() {
        ArrayList<TrackerST300> list = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            PreparedStatement st = connection.prepareStatement("SELECT number_id, content FROM message_received WHERE processed=0");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                String id = rs.getString("number_id");
                String content = rs.getString("content");
                TrackerST300 track = new TrackerST300(content, id);
                list.add(track);
            }
            rs.close();
            st.close();
        } catch (SQLException e) {
            System.out.println("Connection failure");
        }
        return list;
    }

}
