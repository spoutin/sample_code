# How to use
This project can be run via:

`poetry run create_report`

provided the appropriate environment variables are set in a `.env` file.

Alternatively, the project can also be built into a package and the package file installed in another machine:

```
poetry build
# distribute package file to another machine
pip install dist/create_report*.whl
create_report
```

Another way, is to run directly from git using pipx. This is useful for usage in pipelines:

```
pipx run --spec git+https://<link to git repo> create_report
```

