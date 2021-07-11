package ceng.ceng351.cengtubedb;
import java.sql.*;
import java.util.ArrayList;

public class CengTubeDB implements ICengTubeDB{

    Connection connection = null;

    @Override
    public void initialize() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(
                    "Some database connection info."
                    );
        } catch (Exception e) {
            System.out.println("Initialize Error Occurred");
        }
    }

    @Override
    public int createTables() {
        Statement statement;
        int createdTables = 0;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Create Tables - Create Statement Error");
            return createdTables;
        }
        String userTable = "CREATE TABLE IF NOT EXISTS User (" +
                "userID INTEGER ," +
                "userName VARCHAR(30)," +
                "email VARCHAR(30)," +
                "password VARCHAR(30)," +
                "status VARCHAR(30)," +
                "PRIMARY KEY (userID)" +
                ");";
        String videoTable = "CREATE TABLE IF NOT EXISTS Video (" +
                "videoID INTEGER ," +
                "userID INTEGER," +
                "videoTitle VARCHAR(60)," +
                "likeCount INTEGER," +
                "dislikeCount INTEGER," +
                "datePublished DATE," +
                "PRIMARY KEY (videoID)," +
                "FOREIGN KEY (userID) REFERENCES User(userID) on DELETE CASCADE on UPDATE CASCADE" +
                ");";
        String commentTable = "CREATE TABLE IF NOT EXISTS Comment (" +
                "commentID INTEGER ," +
                "userID INTEGER ," +
                "videoID INTEGER ," +
                "commentText VARCHAR(1000)," +
                "dateCommented DATE," +
                "PRIMARY KEY (commentID)," +
                "FOREIGN KEY (userID) REFERENCES User(userID) on DELETE SET NULL on UPDATE CASCADE," +
                "FOREIGN KEY (videoID) REFERENCES Video(videoID) on DELETE CASCADE on UPDATE CASCADE" +
                ");";
        String watchTable = "CREATE TABLE IF NOT EXISTS Watch (" +
                "userID INTEGER ," +
                "videoID INTEGER ," +
                "dateWatched DATE," +
                "PRIMARY KEY (userID,videoID)," +
                "FOREIGN KEY (userID) REFERENCES User(userID) on DELETE CASCADE on UPDATE CASCADE," +
                "FOREIGN KEY (videoID) REFERENCES Video(videoID) on DELETE CASCADE on UPDATE CASCADE" +
                ");";
        try {
            statement.executeUpdate(userTable);
            createdTables++;
        }
        catch (SQLException exception) {
            System.out.println("Create Tables - User Table Error");
        }
        try {
            statement.executeUpdate(videoTable);
            createdTables++;
        }
        catch (SQLException exception) {
            System.out.println("Create Tables - Video Table Error");
        }
        try {
            statement.executeUpdate(commentTable);
            createdTables++;
        }
        catch (SQLException exception) {
            System.out.println("Create Tables - Comment Table Error");
        }
        try {
            statement.executeUpdate(watchTable);
            createdTables++;
        }
        catch (SQLException exception) {
            System.out.println("Create Tables - Watch Table Error");
        }
        return createdTables;
    }

    @Override
    public int dropTables() {
        Statement statement;
        int droppedTables = 0;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Drop Tables - Statement Creation Error");
            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println("Drop Tables - Connection Did Not Closed Properly");
            }
            return droppedTables;
        }
        String commentTable = "DROP TABLE IF EXISTS Comment;";
        String watchTable = "DROP TABLE IF EXISTS Watch;";
        String videoTable = "DROP TABLE IF EXISTS Video;";
        String userTable = "DROP TABLE IF EXISTS User;";
        try {
            statement.executeUpdate(commentTable);
            droppedTables++;
        }
        catch (SQLException exception) {
            System.out.println("Drop Tables - Comment Table Error");
        }
        try {
            statement.executeUpdate(watchTable);
            droppedTables++;
        }
        catch (SQLException exception) {
            System.out.println("Drop Tables - Watch Table Error");
        }
        try {
            statement.executeUpdate(videoTable);
            droppedTables++;
        }
        catch (SQLException exception) {
            System.out.println("Drop Tables - Video Table Error");
        }
        try {
            statement.executeUpdate(userTable);
            droppedTables++;
        }
        catch (SQLException exception) {
            System.out.println("Drop Tables - User Table Error");
        }
        try {
            connection.close();
        } catch (SQLException exception) {
            System.out.println("Drop Tables - Connection Did Not Closed Properly");
        }
        return droppedTables;
    }

    @Override
    public int insertUser(User[] users) {
        int insertedRows = 0;
        for (User user : users) {
            try {
                String insertSQL = "INSERT INTO User Values (?,?,?,?,?)";
                PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
                preparedStatement.setInt(1,user.getUserID());
                preparedStatement.setString(2,user.getUserName());
                preparedStatement.setString(3,user.getEmail());
                preparedStatement.setString(4,user.getPassword());
                preparedStatement.setString(5,user.getStatus());
                preparedStatement.executeUpdate();
                insertedRows++;
            } catch (SQLException exception) {
                System.out.println("Insert User Error");
            }
        }
        return insertedRows;
    }

    @Override
    public int insertVideo(Video[] videos) {
        int insertedRows = 0;
        for (Video video : videos) {
            String insertSQL = "INSERT INTO Video Values(?,?,?,?,?,?);";
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
                preparedStatement.setInt(1,video.getVideoID());
                preparedStatement.setInt(2,video.getUserID());
                preparedStatement.setString(3,video.getVideoTitle());
                preparedStatement.setInt(4,video.getLikeCount());
                preparedStatement.setInt(5,video.getDislikeCount());
                preparedStatement.setDate(6,Date.valueOf(video.getDatePublished()));
                preparedStatement.executeUpdate();
                insertedRows++;
            } catch (SQLException exception) {
                System.out.println("Insert Video Error");
            }
        }
        return insertedRows;
    }

    @Override
    public int insertComment(Comment[] comments) {
        int insertedRows = 0;
        for (Comment comment : comments) {
            String insertSQL = "INSERT INTO Comment Values(?,?,?,?,?);";
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
                preparedStatement.setInt(1,comment.getCommentID());
                preparedStatement.setInt(2,comment.getUserID());
                preparedStatement.setInt(3,comment.getVideoID());
                preparedStatement.setString(4,comment.getCommentText());
                preparedStatement.setDate(5,Date.valueOf(comment.getDateCommented()));
                preparedStatement.executeUpdate();
                insertedRows++;
            } catch (SQLException exception) {
                System.out.println("Insert Comment Error");
            }
        }
        return insertedRows;
    }

    @Override
    public int insertWatch(Watch[] watchEntries) {
        int insertedRows = 0;
        for (Watch watch : watchEntries) {
            String insertSQL = "INSERT INTO Watch Values(?,?,?);";
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
                preparedStatement.setInt(1,watch.getUserID());
                preparedStatement.setInt(2,watch.getVideoID());
                preparedStatement.setDate(3,Date.valueOf(watch.getDateWatched()));
                preparedStatement.executeUpdate();
                insertedRows++;
            } catch (SQLException exception) {
                System.out.println("Insert Watch Error");
            }
        }
        return insertedRows;
    }

    @Override
    public QueryResult.VideoTitleLikeCountDislikeCountResult[] question3() {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 3 - Statement Did not Create");
            return null;
        }
        ArrayList<QueryResult.VideoTitleLikeCountDislikeCountResult> videoTitleLikeCountDislikeCountResults
                = new ArrayList<>();
        String query =  "SELECT DISTINCT v.videoTitle,v.likeCount,v.dislikeCount from Video v " +
                        "WHERE v.likeCount > v.dislikeCount ORDER BY v.videotitle ASC;";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String videoTitle = resultSet.getString("videoTitle");
                int likeCount = resultSet.getInt("likeCount");
                int dislikeCount = resultSet.getInt("dislikeCount");
                QueryResult.VideoTitleLikeCountDislikeCountResult videoTitleLikeCountDislikeCountResult =
                        new QueryResult.VideoTitleLikeCountDislikeCountResult(videoTitle,likeCount,dislikeCount);
                videoTitleLikeCountDislikeCountResults.add(videoTitleLikeCountDislikeCountResult);
            }
        } catch (SQLException exception) {
            System.out.println("Question - 3 - Query Execute Error");
            return null;
        }
        return videoTitleLikeCountDislikeCountResults.toArray(new QueryResult.VideoTitleLikeCountDislikeCountResult[0]);
    }

    @Override
    public QueryResult.VideoTitleUserNameCommentTextResult[] question4(Integer userID) {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 4 - Statement Did not Create");
            return null;
        }
        ArrayList<QueryResult.VideoTitleUserNameCommentTextResult> videoTitleUserNameCommentTextResults =
                new ArrayList<>();
        String query = "SELECT DISTINCT v.videoTitle,u.userName,c.commentText FROM Video v,Comment c,User u " +
                "WHERE v.videoID = c.videoID and c.userID = u.userID AND u.userID = " + userID +" ORDER BY v.videoTitle ASC;";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String videoTitle = resultSet.getString("videoTitle");
                String userName = resultSet.getString("userName");
                String commentText = resultSet.getString("commentText");
                QueryResult.VideoTitleUserNameCommentTextResult videoTitleUserNameCommentTextResult =
                        new QueryResult.VideoTitleUserNameCommentTextResult(videoTitle,userName,commentText);
                videoTitleUserNameCommentTextResults.add(videoTitleUserNameCommentTextResult);
            }
        } catch (SQLException exception) {
            System.out.println("Question - 4 - Query Execute Error");
            return null;
        }
        return videoTitleUserNameCommentTextResults.toArray(new QueryResult.VideoTitleUserNameCommentTextResult[0]);
    }

    @Override
    public QueryResult.VideoTitleUserNameDatePublishedResult[] question5(Integer userID) {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 5 - Statement Did not Create");
            return null;
        }

        ArrayList<QueryResult.VideoTitleUserNameDatePublishedResult> videoTitleUserNameDatePublishedResults =
                new ArrayList<>();
        String query = "SELECT * FROM  (SELECT DISTINCT v.videoTitle,u.userName,v.datePublished  FROM Video v,User u  " +
                "WHERE v.userID = u.userID and u.userID ="+ userID +" and v.videoTitle NOT LIKE '%VLOG%' and v.datePublished <= ALL " +
                "(SELECT v.datePublished FROM Video v,User u  WHERE v.userID = u.userID and u.userID =" + userID + " and v.videoTitle NOT LIKE '%VLOG%') " +
                "ORDER BY v.videoTitle ASC) AS Temp where Temp.videoTitle <= ALL " +
                "(select distinct v.videoTitle from Video v,User u where v.userID = u.userID and u.userID =" + userID + " and v.videoTitle " +
                "NOT LIKE '%VLOG%' and v.datePublished <= ALL (SELECT v.datePublished FROM Video v,User u WHERE v.userID = u.userID and u.userID ="+userID+
                " and v.videoTitle NOT LIKE '%VLOG%') ORDER BY v.videoTitle ASC);";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String videoTitle = resultSet.getString("videoTitle");
                String userName = resultSet.getString("userName");
                String datePublished = resultSet.getDate("datePublished").toString();
                QueryResult.VideoTitleUserNameDatePublishedResult videoTitleUserNameDatePublishedResult =
                        new QueryResult.VideoTitleUserNameDatePublishedResult(videoTitle,userName,datePublished);
                videoTitleUserNameDatePublishedResults.add(videoTitleUserNameDatePublishedResult);
            }
        } catch (SQLException exception) {
            System.out.println("Question - 5 - Query Execute Error");
            return null;
        }
        return videoTitleUserNameDatePublishedResults.toArray(new QueryResult.VideoTitleUserNameDatePublishedResult[0]);
    }

    @Override
    public QueryResult.VideoTitleUserNameNumOfWatchResult[] question6(String dateStart, String dateEnd) {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 6 - Statement Did not Create");
            return null;
        }
        ArrayList<QueryResult.VideoTitleUserNameNumOfWatchResult> videoTitleUserNameNumOfWatchResults =
                new ArrayList<>();
        String query = "SELECT DISTINCT v.videoTitle,u.userName,count(*) as numberOfWatch " +
                "FROM Watch w,Video v,User u " +
                "WHERE v.videoID = w.videoID and v.userID = u.userID and " +
                "w.dateWatched >=" + "'" + dateStart + "'" +" and w.dateWatched <=" + "'" + dateEnd + "'" +
                "GROUP BY w.videoID ORDER BY numberOfWatch DESC LIMIT 3;";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String videoTitle = resultSet.getString("videoTitle");
                String userName = resultSet.getString("userName");
                int numberOfWatch = resultSet.getInt("numberOfWatch");
                QueryResult.VideoTitleUserNameNumOfWatchResult videoTitleUserNameNumOfWatchResult =
                        new QueryResult.VideoTitleUserNameNumOfWatchResult(videoTitle,userName,numberOfWatch);
                videoTitleUserNameNumOfWatchResults.add(videoTitleUserNameNumOfWatchResult);
            }
        } catch (SQLException exception) {
            System.out.println("Question - 6 - Query Execute Error");
            return null;
        }
        return videoTitleUserNameNumOfWatchResults.toArray(new QueryResult.VideoTitleUserNameNumOfWatchResult[0]);
    }

    @Override
    public QueryResult.UserIDUserNameNumOfVideosWatchedResult[] question7() {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 7 - Statement Did not Create");
            return null;
        }
        ArrayList<QueryResult.UserIDUserNameNumOfVideosWatchedResult> userIDUserNameNumOfVideosWatchedResults =
                new ArrayList<>();
        String query = "SELECT DISTINCT Temp2.userID,Temp2.userName,COUNT(*) AS numberOfVideos " +
                "FROM (SELECT u.userID,u.userName FROM User u,Watch w, (SELECT w.videoID,COUNT(*) AS number FROM Watch w GROUP BY w.videoID) AS Temp " +
                "WHERE w.userID=u.userID and w.videoID = Temp.videoID and Temp.number = 1) AS Temp2 GROUP BY Temp2.userID,Temp2.userName ORDER BY Temp2.userID ASC;";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                int userID = resultSet.getInt("userID");
                String userName = resultSet.getString("userName");
                int numberOfVideos = resultSet.getInt("numberOfVideos");
                QueryResult.UserIDUserNameNumOfVideosWatchedResult userIDUserNameNumOfVideosWatchedResult =
                        new QueryResult.UserIDUserNameNumOfVideosWatchedResult(userID,userName,numberOfVideos);
                userIDUserNameNumOfVideosWatchedResults.add(userIDUserNameNumOfVideosWatchedResult);
            }

        } catch (SQLException exception) {
            System.out.println("Question - 7 - Query Execute Error");
            return null;
        }
        return userIDUserNameNumOfVideosWatchedResults.toArray(new QueryResult.UserIDUserNameNumOfVideosWatchedResult[0]);
    }

    @Override
    public QueryResult.UserIDUserNameEmailResult[] question8() {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 8 - Statement Did not Create");
            return null;
        }
        ArrayList<QueryResult.UserIDUserNameEmailResult> userIDUserNameEmailResults = new ArrayList<>();
        String query = "SELECT DISTINCT u.userID,u.userName,u.email FROM User u " +
                "WHERE u.userID IN (SELECT v.userID FROM Video v) AND " +
                "(select count(*) from Watch w,Video v where v.userID = u.userID and w.videoID = v.videoID and w.userID = u.userID) = " +
                "(select count(*) from Video v where v.userID = u.userID) AND " +
                "(select count(distinct(c.videoID)) from Comment c,Video v where v.userID = u.userID and c.videoID = v.videoID and c.userID = u.userID) =" +
                "(select count(*) from Video v where v.userID = u.userID) order by u.userID ASC;";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                int userID = resultSet.getInt("userID");
                String userName = resultSet.getString("userName");
                String email = resultSet.getString("email");
                QueryResult.UserIDUserNameEmailResult userIDUserNameEmailResult =
                        new QueryResult.UserIDUserNameEmailResult(userID,userName,email);
                userIDUserNameEmailResults.add(userIDUserNameEmailResult);
            }
        } catch (SQLException exception) {
            System.out.println("Question - 8 - Query Execute Error");
            return null;
        }
        return userIDUserNameEmailResults.toArray(new QueryResult.UserIDUserNameEmailResult[0]);
    }

    @Override
    public QueryResult.UserIDUserNameEmailResult[] question9() {
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 9 - Statement Did not Create");
            return null;
        }
        ArrayList<QueryResult.UserIDUserNameEmailResult> userIDUserNameEmailResults = new ArrayList<>();
        String query = "SELECT DISTINCT u.userID,u.userName,u.email FROM User u " +
                "WHERE u.userID IN (SELECT w.userID FROM Watch w) AND " +
                "u.userID NOT IN (SELECT c.userID FROM Comment c) ORDER BY u.userID ASC;";
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                int userID = resultSet.getInt("userID");
                String userName = resultSet.getString("userName");
                String email = resultSet.getString("email");
                QueryResult.UserIDUserNameEmailResult userIDUserNameEmailResult =
                        new QueryResult.UserIDUserNameEmailResult(userID,userName,email);
                userIDUserNameEmailResults.add(userIDUserNameEmailResult);
            }
        } catch (SQLException exception) {
            System.out.println("Question - 9 - Query Execute Error");
            return null;
        }
        return userIDUserNameEmailResults.toArray(new QueryResult.UserIDUserNameEmailResult[0]);
    }

    @Override
    public int question10(int givenViewCount) {
        int rowsAffected = 0;
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println("Question - 10 - Statement Did not Create");
            return rowsAffected;
        }
        String query = "UPDATE User u SET u.status = 'verified' " +
                "WHERE (SELECT COUNT(*) FROM Watch w WHERE w.videoID IN (SELECT v.videoID FROM Video v WHERE v.userID = u.userID)) >" + givenViewCount;
        try {
            rowsAffected = statement.executeUpdate(query);
        } catch (SQLException exception) {
            System.out.println("Question - 10 - Query Execute Error");
        }
        return rowsAffected;
    }

    @Override
    public int question11(Integer videoID, String newTitle) {
        int rowsAffected = 0;
        String query = "UPDATE Video SET videoTitle = ? WHERE videoID = ?;";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1,newTitle);
            preparedStatement.setInt(2,videoID);
            rowsAffected = preparedStatement.executeUpdate();
        } catch (SQLException exception) {
            System.out.println("Question - 11 - Query Execute Error");
        }
        return rowsAffected;
    }

    @Override
    public int question12(String videoTitle) {
        String query = "DELETE FROM Video WHERE videoTitle = ?;";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1,videoTitle);
            preparedStatement.executeUpdate();
        } catch (SQLException exception) {
            System.out.println("Question - 12 - Delete Execute Error");
        }
        try {
            Statement statement = connection.createStatement();
            query = "SELECT COUNT(*) as numberOfVideos FROM Video;";
            ResultSet resultSet = statement.executeQuery(query);
            int numberOfVideos = 0;
            while (resultSet.next()) {
                numberOfVideos = resultSet.getInt("numberOfVideos");
            }
            return numberOfVideos;
        } catch (SQLException exception) {
            System.out.println("Question - 12 - Count Execute Error");
            return 0;
        }
    }
}