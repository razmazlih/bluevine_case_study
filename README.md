# Bluevine Case Study

## The Task
In this assignment, you will explore data from some top-ranked sci-fi and fantasy authors’ books.  
You must use **Python** to implement your solution. We strongly suggest using **requests** for the data acquisition part and **pandas** for the data analysis part of the assignment.  

We are interested to see your **entire process** and not just the end result. Please make sure to document all of the relevant work that you do using comments or any other method of your choosing. You will need to make some decisions and assumptions as you solve the assignment—make sure to document these as well.  

This task is intended to be an opportunity for you to showcase both your skills and your general approach to data analysis and engineering problems.

---

## Allotted Time
- The assignment was designed to take **no more than 4 hours** for those familiar with Python and pandas.  
- You should return your final products **within 2 days** of receiving this assignment.

---

## The Dataset
- Use the **Open Library Books API** (no registration required) to fetch data for all books with ISBN numbers listed in `books-isbns.txt`.  
- Not all ISBNs will be available or complete—that’s fine.  
- Because fetching data can be slow, **cache** your results locally (e.g. in JSON) so you can reuse them during development.  
- Add a reasonable **timeout** (1–2 seconds) to your API calls to avoid hanging.

> **Note:** Different editions of the same text have different ISBNs. Treat titles that are identical as the **same book**, even if their ISBNs differ.

---

## The Questions
1. **How many different books** are in the list?  
2. **What is the book with the most number of different ISBNs?**  
3. **How many books don’t have a Goodreads ID?**  
4. **How many books have more than one author?**  
5. **What is the number of books published per publisher?**  
6. **What is the median number of pages** for books in this list?  
7. **What is the month** with the most number of published books?  
8. **What is/are the longest word(s)** that appear(s) either in a book’s description or in the first sentence of a book? In which book(s) (title) does it/they appear?  
9. **What was the last book published** in the list?  
10. **What is the year of the most updated entry** in the list?  
11. **What is the title of the second published book** for the author with the highest number of different titles in the list?  
12. **What is the pair (publisher, author)** with the highest number of books published?

> If you can’t answer one of the questions, please submit your partial answer and move on to the next one. There will be time to discuss any issues in the follow-up meeting.

---

## What to Submit
Create a **ZIP file** containing:

1. `solution.py`  
   - A Python script that can be run and will output the answers to all the questions (printed as logs).  
   - Include any additional code or text files required to run your solution.  
2. `instructions.txt`  
   - Clear instructions on how to install dependencies and run your code.  
3. `answers.txt`  
   - The full console output produced when running `solution.py`.  
4. **[Optional]** Jupyter/Colab notebook  
   - If you used a notebook, feel free to include it—but **still** provide files 1–3 above.

---

*Good luck, and happy coding!*  