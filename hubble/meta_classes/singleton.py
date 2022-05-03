class Singleton(type):

    """Singleton Design Pattern"""

    _instance = {}

    def __call__(cls, *args, **kwargs):

        """if instance already exist dont create one"""

        if cls not in cls._instance:
            cls._instance[cls] = super(Singleton, cls).__call__(*args, **kwargs)
            return cls._instance[cls]
