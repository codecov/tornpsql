from setuptools import setup

setup(name='tornpsql',
      version='2.0.0a2',
      description="PostgreSQL handler for Tornado Web",
      long_description="",
      classifiers=["Development Status :: 5 - Production/Stable",
                   "License :: OSI Approved :: Apache Software License",
                   "Programming Language :: Python :: 2.7",
                   "Programming Language :: SQL",
                   "Topic :: Database"],
      keywords='tornado psql postgres postgresql sql',
      author='@iopeak',
      author_email='steve@stevepeak.net',
      url='https://github.com/stevepeak/tornpsql',
      license='Apache v2.0',
      packages=['tornpsql'],
      include_package_data=True,
      zip_safe=True,
      install_requires=["psycopg2>=2.5.2"],
      entry_points="")
