import nox


@nox.session(python=["3.8", "3.7"])
def tests(session):
    session.install("-r", "requirements.txt")
    session.install(".")
    session.run('pytest')
