def run(age: int, country: str, **kwargs: dict) -> None:
    print("{} years old man from {}, {}".format(age, country, str(kwargs)))
