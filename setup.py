from setuptools import setup

setup(
    name='bamboo',
    version=0.1,
    author='Aaron Mangum',
    author_email='aaron.mangum@juvo.com',
    url='https://github.com/juvoinc/bamboo',
    packages=['bamboo'],
    python_requires='>=2.7',
    setup_requires=["pytest-runner", "flake8"],
    tests_require=["pytest", "pytest-cov"],
    install_requires=[
        'elasticsearch>=6.0.0,<7.0.0',
        'pandas'
    ]
)
