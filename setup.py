from setuptools import setup

setup(
    name="filesystembackup",
    version="0.1",
    description="Can extract files from directory, filter them and generate a ZIP",
    url="https://github.com/joshkreud/PythonFilesystemBackup",
    author="Joshua Kreuder",
    author_email="Joshua_Kreuder@Outlook.com",
    license="MIT",
    packages=["filesystembackup"],
    zip_safe=False,
    install_requires=["pandas",],
)
