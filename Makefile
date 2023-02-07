PYTHON=python3
VENV_NAME=.venv

setup-venv:
	${PYTHON} -m venv ${VENV_NAME} && \
	. ${VENV_NAME}/bin/activate && \
	pip install --upgrade pip && \
	pip install -e .

test: setup-venv
	. ${VENV_NAME}/bin/activate && \
	pip install pytest && \
	pytest -v

dist: setup-venv
	. ${VENV_NAME}/bin/activate && \
	pip install wheel && \
	${PYTHON} setup.py sdist bdist_wheel


