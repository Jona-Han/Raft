# General Guidelines for Each Lab

1. Ensure that your code both **compiles** and **runs** on the undergraduate servers. Failure to compile successfully will result in a zero for the lab.

1. **Automated testing.** All test cases carry equal weight in grading. If your program passes 3 out of 10 tests for a lab, you will receive at most 30% of the grade for that lab. Any runtime errors leading to test failures are considered as **test not passed**.
   - We will run each test multiple times, and failing any of them will fail that particular test. For instance, depending on the nature of a test, we may decide to run it for 100 times.
   - While we plan to share all tests in advance, we may occassionally also run hidden tests. In this case, we will share the test used for evaluation afterwards.

1. **Oral examination.** We may conduct an oral examination for each lab. For team submissions, we will evaluate both team members together. In each case, 50% of the grades will be derived from automated testing and 50% from the oral examination.

1. Stick to the provided Go built-in packages for coding; do not use external packages unless explicitly allowed. Using an external package for a lab will result in a zero for that lab.

1. Each lab provides a basic template to start your work. Ideally, make changes only to the files required. There are sufficient places where you can put your code to get full marks for the lab. Please refrain from changing the specified test files and the directory structure. Our testing scripts will use pristine copies of the test files and will rely on the original directory structure.

1. For debugging pursposes, you may need to make changes to files specified as unchangeable, e.g., you may decide to comment out or modify the testing functions to understand exactly where your code is failing. Feel free to make such changes. However, ensure that your program ultimately compiles and runs with the original versions of these files.

# Your Repository MUST NOT Be Public
Make sure your work is private to you and your teammate. Your code must not be made available to the public. If you fork this repository, ensure that it does not contain any solutions.

This is a suggested approach for collaboration. You can use any other method to keep it private:

- Create a private repo (add your teammate as your member).
- Clone the public repo.
- Add a new remote to your private repository.
- Push your changes to the private repo and collaborate with your teammate.
- For each lab, pull from the public repository.

# Submitting Your Code

You will submit your solutions on **Canvas**. When submitting your code:

- Use `make tar lab_name=LAB_NAME student_id=STUDENT_ID` to create a `.tar.gz` version for your lab. The `STUDENT_ID` should adhere to the naming pattern:
  - For lab 1: `i_[stu-number]`
  - For lab 2-4: `g_[stu-number1]_[stu-number2]`
  - Submit the `tar` file on Canvas.

   The following is an example of how to do it on the `pender` machine (assuming the repository is already cloned in the home directory and lab1 is completed):

```bash
sinaee@pender:~$ ls
cpsc416-2023w2-golabs

sinaee@pender:~$ cd cpsc416-2023w2-golabs

sinaee@pender:~$ ls
Makefile  README.md src docs

sinaee@pender:~$ make tar lab_name=lab1 student_id=i_87654321
# NOTE: this is the `make tar` command and not the `tar` command. See `NOTICE 1` below.
# a lot of output; you should not see any errors.

sinaee@pender:~$ ls
Makefile          README.md         i_87654321.tar.gz   src     docs
```

Now, upload the `i_87654321.tar.gz` on **Canvas** for the corresponding assignment. If we cannot untar your file, we cannot evaluate your lab. Therefore, ensure that the `tar` object is created successfully by simply downloading your submission and untarring it. Use the following command to untar your files.

```bash
sinaee@pender:~$ tar xzvf i_87654321.tar.gz
```

**NOTICE 1:** We use the `make tar` command defined in the `Makefile`, which automatically excludes unnecessary files and folders, such as the `.git` folder. If you execute the `tar` command directly, it would include the entire Git history, which is something we aim to avoid.

**NOTICE 2:** We would like to re-emphasize that you should test and run your code on one of the undergrad servers. For instance, in the above example, we used the `pender` machine.

# Labs

- See [Lab 1 instructions](docs/lab1.md)
- See [Lab 2 instructions (lab2A, lab2B, lab2C, lab2D)](docs/lab2.md)

# Acknowledgements
The programming labs are based on the labs developed by Robert Morris, Frans Kaashoek, and Nickolai Zeldovich at MIT as part of their graduate course on Distributed Systems (6.584), and are reused with permission from the content authors.
