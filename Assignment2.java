import java.sql.*;
import java.util.*;

public class Assignment2 extends JDBCSubmission {

    PreparedStatement ps;
    ResultSet rs;

    public Assignment2() throws ClassNotFoundException {
        // Initialization
        try {
            Class.forName("org.postgresql.Driver");
            connection = null;
            ps = null;
            rs = null;
        } catch (ClassNotFoundException e) {
            System.out.println("Failed to find the JDBC driver");
        }
    }

    @Override
    public boolean connectDB(String url, String username, String password) {
        // Set up connection
        try {
            connection = DriverManager.getConnection(url, username, password);
            if(connection != null) { return(true); }
            else { return(false); }
        } catch (SQLException e) {
            return(false);
        }
    }

    @Override
    public boolean disconnectDB() {
        // Close PreparedStatement
        if(ps != null) {
            try { 
                ps.close();
            } catch (SQLException e) {
                System.out.println("Failed to disconnect ps");
                return(false);
            }
        }

        // Close ResultSet
        if(rs !=  null) {
            try {
                rs.close();
            } catch (SQLException e) {
                System.out.println("Failed to disconnect rs");
                return(false);
            }
        }

        // Close Connection
        if(connection !=  null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println("Failed to disconnect conn");
                return(false);
            }
        }
        return true;
    }

    @Override
    public ElectionCabinetResult electionSequence(String countryName) {
        // Structures to fill ElectionCabinetResult
        List<Integer> electionsList = new ArrayList<Integer>();
        List<Integer> cabinetsList = new ArrayList<Integer>();
        try {
            // Set search path to parlgov, clear old temp views
            Statement state = connection.createStatement();
            state.execute("SET SEARCH_PATH TO 'parlgov'");
            state = connection.createStatement();
            String dropView = "DROP VIEW IF EXISTS beforeElection";
            state.executeUpdate(dropView);
            dropView = "DROP VIEW IF EXISTS afterElection";
            state.executeUpdate(dropView);
            dropView = "DROP VIEW IF EXISTS allMostRecentElections";
            state.executeUpdate(dropView);
            dropView = "DROP VIEW IF EXISTS allElections";
            state.executeUpdate(dropView);


            // Find id of countryName
            String queryString = "SELECT id FROM country WHERE name = ?";
            ps = connection.prepareStatement(queryString);
            ps.setString(1, countryName);
            rs = ps.executeQuery();
            rs.next();
            int countryId = rs.getInt("id");


            // Find all elections, when they first started, and when they were proceeded by next election
            queryString = "CREATE VIEW allElections AS SELECT T1.id, T1.e_date::date AS startDate, T2.e_date::date AS endDate " + 
            " FROM" +
            " election T1, election T2 " +
            " WHERE" + 
            " T1.country_id = " + countryId + " AND T1.country_id = T2.country_id AND" + 
            " T1.e_type = T2.e_type AND T1.e_date < T2.e_date AND" +
            " T2.e_date <= ALL (SELECT T3.e_date from election T3 where T3.e_date > T1.e_date AND T3.country_id = T1.country_id AND T3.e_type = T1.e_type) " + 
            " ORDER BY " + 
            " date_part('year', T1.e_date) DESC; ";
            state.executeUpdate(queryString);


            // Unionize previous query with NULL end_dates (i.e. latest elections)
            String queryStringTwo = "CREATE VIEW allMostRecentElections AS SELECT id, startDate::date, endDate::date " + 
            " FROM ((SELECT id, e_date::date AS startDate, NULL::date AS endDate FROM election" +
            " WHERE id NOT IN (SELECT id FROM allElections) AND country_id = " + countryId +") UNION (SELECT * FROM allElections))" +
            " T3 ORDER BY date_part('year', endDate) DESC";
            state.executeUpdate(queryStringTwo);


            // Find election, cabinet pairs where cabinet started after election
            String queryStringThree = "CREATE VIEW afterElection AS SELECT allMostRecentElections.id AS electionId, cabinet.id AS cabinetId, start_date::date, startDate::date, endDate::date " +
            " FROM allMostRecentElections, cabinet " +
            " WHERE cabinet.start_date::date >= startDate::date AND " +  "cabinet.election_id = allMostRecentElections.id AND" +
            " cabinet.country_id = " + countryId + " ORDER BY date_part('year', endDate) DESC NULLS LAST";
            state.executeUpdate(queryStringThree);


            // Find election, cabinet pairs where cabinet started before election
            queryStringThree = "CREATE VIEW beforeElection AS SELECT allMostRecentElections.id AS electionId, cabinet.id AS cabinetId, start_date::date, startDate::date, endDate::date " +
            " FROM allMostRecentElections, cabinet " +
            " WHERE (endDate::date IS NULL OR cabinet.start_date::date < endDate::date) AND " +
            " cabinet.country_id = " + countryId + " AND cabinet.election_id = allMostRecentElections.id" +
            " ORDER BY date_part('year', endDate) DESC NULLS LAST";
            state.executeUpdate(queryStringThree);


            // Find intersection of both sets (final result)
            queryStringThree = "SELECT * FROM " + 
             " (select * from afterElection intersect select * from beforeElection) allCabinetElectionCombos " +
             " ORDER BY date_part('year', endDate) DESC";


            // Execute the query
            ps = connection.prepareStatement(queryStringThree);
            rs = ps.executeQuery();


            // Fill result lists
            while(rs.next()) {
                electionsList.add(rs.getInt("electionId"));
                cabinetsList.add(rs.getInt("cabinetId")); 
            }

            // Form return result, and close off temp variables
            ElectionCabinetResult myResult = new ElectionCabinetResult(electionsList, cabinetsList);
            state.close();
            return(myResult);
        }
        catch (SQLException se) {
                        System.err.println("SQL Exception." +
                                "<Message>: " + se.getMessage());
                    return(null);
                }
    }

    @Override
    public List<Integer> findSimilarPoliticians(Integer politicianId, Float threshold) {
        try {
            // Set search path to parlgov
            Statement state = connection.createStatement();
            state.execute("SET SEARCH_PATH TO 'parlgov'");
            state = connection.createStatement();

            // Find the comment and description of the given politicianId
            String queryString = "SELECT comment, description FROM politician_president WHERE id = ?";
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, politicianId);
            rs = ps.executeQuery();
            rs.next();
            String polComment = rs.getString("comment");
            String polDesc = rs.getString("description");

            // Form return list and find all politicians that have Jaccard similarity greater than threshold
            List<Integer> returnList = new LinkedList<Integer>();
            queryString = "SELECT id, comment, description FROM politician_president WHERE id != ?";
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, politicianId);
            rs = ps.executeQuery();
            while(rs.next()) {
                if(similarity(polComment + " " + polDesc, rs.getString("comment") + " " + rs.getString("description")) > threshold) {
                    returnList.add(rs.getInt("id"));
                }
            }

            // Close temp variables, return the list of identical politicians
            state.close();
            return (returnList);
        } catch (SQLException se) {
                System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
            return(null);
        }
    }

    public static void main(String[] args) {
        // You can put testing code in here. It will not affect our autotester.
        System.out.println("Hello");
        Assignment2 my_db = null;
        try {
            my_db = new Assignment2();
            my_db.connectDB("jdbc:postgresql://localhost:5432/csc343h-solomahy", "solomahy", "");
        } catch (ClassNotFoundException e) { }

        // Confirm SimilarPoliticians
        List<Integer> polList = my_db.findSimilarPoliticians(148, (float) 0);
        for (Integer s : polList) {
            System.out.println("Related: " + s);
        }
        polList = my_db.findSimilarPoliticians(113, (float) 0);
        for (Integer s : polList) {
            System.out.println("Related: " + s);
        }

        // Confirm ElectionSequence
        ElectionCabinetResult myResult = my_db.electionSequence("Canada");
        for(int i = 0; i < myResult.elections.size(); i++) {
            System.out.println("Index election: " + i + " - " + myResult.elections.get(i));
            System.out.println("Index cabinet: " + i + " - " + myResult.cabinets.get(i));
        }
        my_db.disconnectDB();
    }
}
