import logging

from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


def count_opportunities_since(conn, since_datetime):
    """
    Count opportunities created since the given datetime.

    Args:
        conn: SQLAlchemy connection
        since_datetime: Datetime to count from

    Returns:
        Count of opportunities
    """
    try:
        query = """
        SELECT COUNT(*) FROM trade_opportunities 
        WHERE opportunity_timestamp >= :since
        """
        result = conn.execute(text(query), {"since": since_datetime}).scalar()
        return result or 0
    except Exception as e:
        logger.error(f"Error counting recent opportunities: {e}")
        return 0


def count_executions_since(conn, since_datetime):
    """
    Count executions created since the given datetime.

    Args:
        conn: SQLAlchemy connection
        since_datetime: Datetime to count from

    Returns:
        Count of executions
    """
    try:
        query = """
        SELECT COUNT(*) FROM trade_executions 
        WHERE execution_timestamp >= :since
        """
        result = conn.execute(text(query), {"since": since_datetime}).scalar()
        return result or 0
    except Exception as e:
        logger.error(f"Error counting recent executions: {e}")
        return 0


def count_profitable_executions(conn, strategy_name=None):
    """
    Count profitable trade executions, optionally filtered by strategy.

    Args:
        conn: SQLAlchemy connection
        strategy_name: Optional strategy name to filter by

    Returns:
        Count of profitable executions
    """
    try:
        query = """
        SELECT COUNT(*) FROM trade_executions 
        WHERE executed_sell_price > executed_buy_price
        """
        params = {}

        if strategy_name:
            query += " AND strategy = :strategy"
            params["strategy"] = strategy_name

        result = conn.execute(text(query), params).scalar()
        return result or 0
    except Exception as e:
        logger.error(f"Error counting profitable executions: {e}")
        return 0


def get_recent_opportunities(conn, limit=10, strategy=None):
    """
    Get recent trade opportunities, optionally filtered by strategy.

    Args:
        conn: SQLAlchemy connection
        limit: Maximum number of opportunities to return
        strategy: Optional strategy name to filter by

    Returns:
        List of opportunity dictionaries
    """
    try:
        query = """
        SELECT * FROM trade_opportunities 
        """
        params = {}

        if strategy:
            query += " WHERE strategy = :strategy"
            params["strategy"] = strategy

        query += " ORDER BY opportunity_timestamp DESC LIMIT :limit"
        params["limit"] = limit

        result = conn.execute(text(query), params).fetchall()

        # Convert rows to dictionaries
        opportunities = []
        for row in result:
            opp_dict = row._asdict()
            # Convert timestamp to datetime
            if "opportunity_timestamp" in opp_dict:
                opp_dict["created_at"] = opp_dict["opportunity_timestamp"]
            # Convert UUID to string
            if "id" in opp_dict:
                opp_dict["id"] = str(opp_dict["id"])
            opportunities.append(opp_dict)

        return opportunities
    except Exception as e:
        logger.error(f"Error getting recent opportunities: {e}")
        return []


def get_recent_executions(conn, limit=10, strategy=None):
    """
    Get recent trade executions, optionally filtered by strategy.

    Args:
        conn: SQLAlchemy connection
        limit: Maximum number of executions to return
        strategy: Optional strategy name to filter by

    Returns:
        List of execution dictionaries
    """
    try:
        query = """
        SELECT * FROM trade_executions 
        """
        params = {}

        if strategy:
            query += " WHERE strategy = :strategy"
            params["strategy"] = strategy

        query += " ORDER BY execution_timestamp DESC LIMIT :limit"
        params["limit"] = limit

        result = conn.execute(text(query), params).fetchall()

        # Convert rows to dictionaries
        executions = []
        for row in result:
            exec_dict = row._asdict()
            # Convert timestamp to datetime
            if "execution_timestamp" in exec_dict:
                exec_dict["created_at"] = exec_dict["execution_timestamp"]
            # Convert UUID to string
            if "id" in exec_dict:
                exec_dict["id"] = str(exec_dict["id"])
            if "opportunity_id" in exec_dict and exec_dict["opportunity_id"]:
                exec_dict["opportunity_id"] = str(exec_dict["opportunity_id"])
            executions.append(exec_dict)

        return executions
    except Exception as e:
        logger.error(f"Error getting recent executions: {e}")
        return []


def get_executions_between(conn, start_date, end_date):
    """
    Get executions between two dates.

    Args:
        conn: SQLAlchemy connection
        start_date: Start datetime
        end_date: End datetime

    Returns:
        List of execution dictionaries
    """
    try:
        query = """
        SELECT * FROM trade_executions 
        WHERE execution_timestamp BETWEEN :start_date AND :end_date
        ORDER BY execution_timestamp ASC
        """
        params = {"start_date": start_date, "end_date": end_date}

        result = conn.execute(text(query), params).fetchall()

        # Convert rows to dictionaries
        executions = []
        for row in result:
            exec_dict = row._asdict()
            # Convert timestamp to datetime
            if "execution_timestamp" in exec_dict:
                exec_dict["created_at"] = exec_dict["execution_timestamp"]
            # Convert UUID to string
            if "id" in exec_dict:
                exec_dict["id"] = str(exec_dict["id"])
            if "opportunity_id" in exec_dict and exec_dict["opportunity_id"]:
                exec_dict["opportunity_id"] = str(exec_dict["opportunity_id"])
            executions.append(exec_dict)

        return executions
    except Exception as e:
        logger.error(f"Error getting executions between dates: {e}")
        return []
