import com.hazelcast.map.MapStore;
import java.sql.*;
import java.util.*;

public class CounterMapStore implements MapStore<Integer, Integer> {
    private Connection con;

    // public CounterMapStore() {
    //     try {
    //         // Підключаємося до контейнера postgres
    //         con = DriverManager.getConnection("jdbc:postgresql://postgres:5432/postgres", "user", "password");
    //         // Створюємо таблицю, якщо її немає
    //         Statement stmt = con.createStatement();
    //         stmt.executeUpdate("CREATE TABLE IF NOT EXISTS counters (id INT PRIMARY KEY, val INT)");
    //     } catch (SQLException e) {
    //         e.printStackTrace();
    //     }
    // }
    public CounterMapStore() {
        String url = "jdbc:postgresql://postgres:5432/counter_db";
        String user = "postgres";
        String password = "postgres";
        
        // Пробуємо підключитися 20 разів з паузою в 2 секунди
        for (int i = 0; i < 20; i++) {
            try {
                con = DriverManager.getConnection(url, user, password);
                // Якщо дійшли сюди - успіх!
                System.out.println(">>> Connected to PostgreSQL successfully!");
                
                Statement stmt = con.createStatement();
                stmt.executeUpdate("CREATE TABLE IF NOT EXISTS counters (id INT PRIMARY KEY, val INT)");
                return; // Виходимо з конструктора
            } catch (SQLException e) {
                System.out.println(">>> Waiting for PostgreSQL... Attempt " + (i + 1));
                try {
                    Thread.sleep(2000); // Чекаємо 2 секунди
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
        // Якщо за 40 секунд не підключилися - кидаємо фатальну помилку
        throw new RuntimeException("Could not connect to PostgreSQL after multiple attempts");
    }

    @Override
    public synchronized void store(Integer key, Integer value) {
        try {
            // Write-through: Upsert (Insert or Update)
            PreparedStatement stmt = con.prepareStatement(
                "INSERT INTO counters (id, val) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
            stmt.setInt(1, key);
            stmt.setInt(2, value);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void storeAll(Map<Integer, Integer> map) {
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void delete(Integer key) {
        try {
            PreparedStatement stmt = con.prepareStatement("DELETE FROM counters WHERE id = ?");
            stmt.setInt(1, key);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void deleteAll(Collection<Integer> keys) {
        for (Integer key : keys) delete(key);
    }

    @Override
    public synchronized Integer load(Integer key) {
        try {
            // Read-through: Якщо в кеші немає, читаємо з БД
            PreparedStatement stmt = con.prepareStatement("SELECT val FROM counters WHERE id = ?");
            stmt.setInt(1, key);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("val");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized Map<Integer, Integer> loadAll(Collection<Integer> keys) {
        Map<Integer, Integer> result = new HashMap<>();
        for (Integer key : keys) {
            Integer value = load(key);
            if (value != null) result.put(key, value);
        }
        return result;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        return null; // Можна реалізувати для "прогріву" кешу
    }
}