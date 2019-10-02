package trackerserverutfpr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TrackerPack implements Runnable {

    private ArrayList<TrackerST300> listProcess;
    private ArrayBlockingQueue<TrackerST300> listMsgsProcessed;
    
    public TrackerPack (ArrayList<TrackerST300> listProcess){
        this.listProcess = listProcess;
        this.listMsgsProcessed = new ArrayBlockingQueue<>(this.listProcess.size());
    }
    
    @Override
    public void run() {
        
        //UPDATE PARA ESTADO PROCESSANDO (processed=2)
        try {
            updateToProcess(this.listProcess);
        } catch (SQLException ex) {
            Logger.getLogger(TrackerPack.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        //PROCESSA MENSAGENS
        this.listProcess.forEach((track) -> {
            Thread thread = null;                
            TrackerST300 tracker = new TrackerST300(track.getMsgcomplet(), this.listMsgsProcessed, track.getIdDB());
            thread = new Thread(tracker);
            thread.start();      
            //BLOQUEAR ATE PROCESSAR TODAS MENSAGENS       
            try {
                thread.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(TrackerPack.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
                
        //REMOVE TODAS MENSAGENS PROCESSADAS DO ARRAY COMPARTILHADO
        ArrayList<TrackerST300> listProcessed = removeMsgsProcessed();

        //INSERE TODAS MENSAGENS PROCESSADAS NO BANCO E ATUALIZA MSGS NAO PROCESSADAS PARA PROCESSADAS        
        int[] retUpdate = null;
        try {
            insertAndUpdateMsgsProcessed(listProcessed, this.listProcess);
        } catch (SQLException | ParseException ex) {
            Logger.getLogger(TrackerPack.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void updateToProcess(ArrayList<TrackerST300> list) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement("UPDATE message_received set processed=2 where number_id=?");
            for (TrackerST300 tracker : list) {
                ps.setInt(1, Integer.parseInt(tracker.getIdDB()));
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
        }
    }
    
    private ArrayList<TrackerST300> removeMsgsProcessed() {
        ArrayList<TrackerST300> listForProcessed = new ArrayList<>();
        this.listMsgsProcessed.drainTo(listForProcessed);
        return listForProcessed;
    }
    
    private void insertAndUpdateMsgsProcessed(ArrayList<TrackerST300> listProcessed, ArrayList<TrackerST300> list) throws SQLException, ParseException {
        int[] retUpdate;
        System.out.println("Size list msgs processed: " + listProcessed.size());
        System.out.println("Size list update: " + list.size());
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement("INSERT INTO message_processed (tracker_id, time, latitude, longitude, time_receive) VALUES (?, ?, ?, ?, ?)");
            for (TrackerInterface tracker : listProcessed) {
                Calendar c = Calendar.getInstance();
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                c.setTime(format.parse(tracker.getDateTime()));
                Timestamp stamp = new Timestamp(c.getTimeInMillis());
                ps.setString(1, tracker.getIdTracker());
                ps.setTimestamp(2, stamp);
                ps.setString(3, tracker.getLatitude());
                ps.setString(4, tracker.getLongitude());
                Calendar calendar = Calendar.getInstance();
                java.util.Date now = calendar.getTime();
                java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
                ps.setTimestamp(5, currentTimestamp);
                ps.addBatch();
            }
            int[] retInsert = ps.executeBatch();
            PreparedStatement ps2 = connection.prepareStatement("UPDATE message_received set processed=1 where number_id=?");
            for (TrackerST300 tracker : list) {
                ps2.setInt(1, Integer.parseInt(tracker.getIdDB()));
                ps2.addBatch();
            }
            retUpdate = ps2.executeBatch();
            connection.commit();
            System.out.println("TrackPacket "+Thread.currentThread().getId()+ " size Insert: " + retInsert.length + " size Update: " + retUpdate.length);
        }
    }
}