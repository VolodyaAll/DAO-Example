package ru.itsphere.dbworkwithdao.dao;

import ru.itsphere.dbworkwithdao.domain.Author;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * Реализация AuthorDao через JDBC.
 * <p>
 * http://it-channel.ru/
 *
 * @author Budnikov Aleksandr
 */
public class AuthorDaoJdbcImpl implements AuthorDao {

    public static final String SELECT_BY_ID_QUERY = "SELECT * FROM authors WHERE id = ?";
    public static final String INSERT_AUTHOR = "INSERT INTO Authors (NAME, TRADE_UNION) VALUES (?, ?);";
    public static final String SELECT_ALL_AUTHORS = "SELECT * FROM Authors";
    public static final String UPDATE_BY_ID = "UPDATE Authors SET name = ?, trade_union = ? WHERE id = ?;";
    public static final String DELETE_BY_ID = "DELETE FROM Authors WHERE id = ?;";
    public static final String DELETE_ALL_AUTHORS = "DELETE FROM Authors;";
    public static final String COLUMN_ID = "id";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_UNION = "trade_union";
    private ConnectionFactory connectionFactory;

    public AuthorDaoJdbcImpl(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public Author getById(long id) {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_ID_QUERY);) {
            statement.setLong(1, id);
            try (ResultSet resultSet = statement.executeQuery();) {
                while (resultSet.next()) {
                    return new Author(resultSet.getLong(COLUMN_ID),
                            resultSet.getString(COLUMN_NAME),
                            resultSet.getString(COLUMN_UNION));
                }
            }
        } catch (Exception e) {
            throw new DaoException(String.format("Method getById(id: '%d') has thrown an exception.", id), e);
        }
        return null;
    }

    @Override
    public void insert(Author author)
    {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT_AUTHOR))
        {
            statement.setString(1, author.getName());
            statement.setString(2, author.getTradeUnion());

            int changeRows = statement.executeUpdate();
            if (changeRows > 0) System.out.println("Insert is successful");
            else System.out.println("Insert isn't successful");
        }
        catch (Exception e)
        {
            throw new DaoException(String.format("Method insert(Author: '%s') has thrown an exception.", author.getName()), e);
        }
    }

    @Override
    public List<Author> getAll()
    {
        try (Connection connection = connectionFactory.getConnection();
             Statement statement = connection.createStatement())
        {
            List<Author> authors = new ArrayList<>();
            ResultSet resultSet = statement.executeQuery(SELECT_ALL_AUTHORS);

            while (resultSet.next())
                authors.add(new Author(resultSet.getLong(COLUMN_ID),
                        resultSet.getString(COLUMN_NAME),
                        resultSet.getString(COLUMN_UNION)));

            return authors;
        }
        catch (Exception e)
        {
            throw new DaoException("Method getAll() has thrown an exception.", e);
        }
    }

    @Override
    public void update(Author author)
    {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPDATE_BY_ID))
        {

            statement.setString(1, author.getName());
            statement.setString(2, author.getTradeUnion());
            statement.setLong(3, author.getId());
            statement.execute();
        }
        catch (Exception e)
        {
            throw new DaoException(String.format("Method update(Author: '%s') has thrown an exception.", author.getName()), e);
        }
    }

    @Override
    public void deleteById(long id)
    {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_BY_ID))
        {
            statement.setLong(1, id);

            int changeRows = statement.executeUpdate();
            if (changeRows > 0) System.out.println("Delete by ID is successful");
            else System.out.println("Delete by ID isn't successful");
        }
        catch (Exception e)
        {
            throw new DaoException(String.format("Method deleteById(Long: '%d') has thrown an exception.", id), e);
        }
    }

    @Override
    public void deleteAll()
    {
        try (Connection connection = connectionFactory.getConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_ALL_AUTHORS))
        {
            int changeRows = statement.executeUpdate();
            if (changeRows > 0) System.out.println("Delete is successful");
            else System.out.println("Delete isn't successful(May be table is empty)");
        }
        catch (Exception e)
        {
            throw new DaoException("Method deleteAll() has thrown an exception.", e);
        }
    }
}
