from datetime import date
import execution as ex


class DBPopulation:
    """
    This class is specially designed for the DB population,
    It takes the preprocessed data and converts it into a form that can be populate to the database
    """
    def __init__(self, date: str = str(date.today())):
        self.date = date
        self.preprocess_data = ex.Execution(self.date).execution_by_extension()


if __name__ == "__main__":
    db = DBPopulation()
    print(db.preprocess_data)

