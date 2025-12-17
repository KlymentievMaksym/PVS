import com.hazelcast.map.MapStore;
import java.sql.*;
import java.util.*;


public class CounterMapStore implements MapStore<Integer, Integer>
{
    private Connection connection;

    public CounterMapStore()
    {
        String url = "jdbc:postgresql://postgres:5432/counter_db";
        String user = "postgres";
        String password = "postgres";
        
        for (int i = 0; i < 20; i++)
            {
            try
            {
                connection = DriverManager.getConnection(url, user, password);

                System.out.println("[INFO] Connected to PostgreSQL");

                Statement statement = connection.createStatement();
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS counters (id INT PRIMARY KEY, value INT)");
                return;
            }
            catch (SQLException e)
            {
                System.out.println(">>> Waiting for PostgreSQL... Attempt " + (i + 1));
                try
                {
                    Thread.sleep(2000);
                }
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }
        }

        throw new RuntimeException("Could not connect to PostgreSQL after multiple attempts");
    }

    @Override
    public synchronized void store(Integer key, Integer value)
    {
        try
        {
            PreparedStatement statement = connection.prepareStatement("INSERT INTO counters (id, value) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value");
            statement.setInt(1, key);
            statement.setInt(2, value);
            statement.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void storeAll(Map<Integer, Integer> map)
    {
        for (Map.Entry<Integer, Integer> entry : map.entrySet())
        {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void delete(Integer key)
    {
        try
        {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM counters WHERE id = ?");
            statement.setInt(1, key);
            statement.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void deleteAll(Collection<Integer> keys)
    {
        for (Integer key : keys) delete(key);
    }

    // Read-through
    @Override
    public synchronized Integer load(Integer key)
    {
        try
        {
            PreparedStatement statement = connection.prepareStatement("SELECT value FROM counters WHERE id = ?");
            statement.setInt(1, key);
            ResultSet rs = statement.executeQuery();
            if (rs.next())
            {
                return rs.getInt("value");
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized Map<Integer, Integer> loadAll(Collection<Integer> keys)
    {
        Map<Integer, Integer> result = new HashMap<>();
        for (Integer key : keys)
        {
            Integer value = load(key);
            if (value != null) result.put(key, value);
        }
        return result;
    }

    @Override
    public Iterable<Integer> loadAllKeys()
    {
        return null;
    }
}