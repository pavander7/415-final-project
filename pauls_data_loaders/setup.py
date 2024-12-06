from setuptools import setup, find_packages

setup(
    name='pauls_data_loaders',  # Name of your package
    version='1.0',             # Version number
    packages=find_packages(),  # This finds all subpackages automatically
    install_requires=[         # List any dependencies your package needs             
        'pandas'              
    ],
    classifiers=[             # Add classifiers to make your package easily searchable
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.12.5',   # Minimum Python version required
)
