import pytest
import testing.postgresql


@pytest.fixture(scope='module')
def postgres(request):
    psql = testing.postgresql.Postgresql()

    def fin():
        psql.stop()

    request.addfinalizer(fin)
    return psql
