from threading import Lock


class Singleton(type):
    _instances = {}
    _lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Model:
    def __iter__(self):  # give you the ability to use this class in a loop
        for attr, value in self.__dict__.items():
            yield attr[attr.rfind('__') + 2:], value

    def __str__(self):
        return f"{type(self).__name__}(\n" + ',\n'.join(
            [f"{attr[attr.rfind('__') + 1:]}={value}" for attr, value in self.__dict__.items()]
        ) + "\n)"

    def __repr__(self):
        return self.__str__()


class A(Model):
    def __init__(self):
        self.__me = 'me'
