"""Install bamboo package."""
from setuptools import setup

setup(
    name='bamboo',
    packages=['bamboo'],
    version='0.2.5',
    license='MIT',
    description='DataFrame interface for ElasticSearch',
    author='Aaron Mangum',
    author_email='aaron.mangum@juvo.com',
    url='https://github.com/juvoinc/bamboo',
    download_url='https://github.com/juvoinc/bamboo/archive/0.2.5.tar.gz',
    keywords=['elasticsearch', 'dataframe', 'pandas'],
    python_requires='>=2.7',
    setup_requires=["pytest-runner", "flake8"],
    tests_require=["pytest", "pytest-cov", "pandas"],
    install_requires=[
        'elasticsearch>=6.0.0'
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ]
)
