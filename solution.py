"""
BlueVine Case Study Solution

This script:
1. Loads a list of ISBNs from a file.
2. Loads or initializes a cache to avoid repeated API calls.
3. Fetches book data from the OpenLibrary API for each ISBN.
4. Normalizes (flattens) the raw API data into consistent records.
5. Builds a pandas DataFrame and computes the answers to all questions.
6. Saves the updated cache and writes out the answers.
"""

import os
import json
import time
import re
from datetime import datetime

import requests
import pandas as pd

# ─── Configuration ────────────────────────────────────────────────────────────────
ISBN_FILE = "books-isbns.txt"
CACHE_FILE = "cache.json"
OUTPUT_FILE = "answers.txt"
API_ENDPOINT = "https://openlibrary.org/api/books"
TIMEOUT = 1.5  # seconds
DELAY = 0.1  # seconds between requests


# ─── Load ISBN list ───────────────────────────────────────────────────────
def load_isbns(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


# ─── Load or initialize cache ─────────────────────────────────────────────
def load_cache(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ─── Fetch raw book data ─────────────────────────────────────────────────
def fetch_book_data(isbn, cache):
    if isbn in cache:
        return cache[isbn]

    try:
        response = requests.get(
            API_ENDPOINT,
            params={"bibkeys": f"ISBN:{isbn}", "format": "json", "jscmd": "data"},
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json().get(f"ISBN:{isbn}")

        if data and "key" in data:
            key = data["key"]
            details_url = f"https://openlibrary.org/{key}.json"
            details_response = requests.get(details_url, timeout=TIMEOUT)
            details_response.raise_for_status()
            details_data = details_response.json()
            data["last_modified"] = details_data.get("last_modified", {})
            data["description"] = details_data.get("description", {})
    except requests.RequestException:
        data = None

    cache[isbn] = data
    time.sleep(DELAY)
    return data


# ─── Normalize (flatten) each record ─────────────────────────────────────
def flatten_record(isbn, data):
    # record includes all fields required to answer questions 1–12.
    record = {
        "isbn": isbn,
        "title": None,
        "authors": [],
        "publishers": [],
        "publish_date": None,
        "number_of_pages": None,
        "goodreads_ids": [],
        "last_modified": None,
        "description": "",
        "first_sentence": "",
    }

    if not data:
        return record

    record["title"] = data.get("title")
    record["authors"] = [
        a.get("name") for a in data.get("authors", []) if a.get("name")
    ]
    record["publishers"] = [
        p.get("name") for p in data.get("publishers", []) if p.get("name")
    ]
    record["publish_date"] = data.get("publish_date")
    record["number_of_pages"] = data.get("number_of_pages")
    record["goodreads_ids"] = data.get("identifiers", {}).get("goodreads", [])
    record["last_modified"] = data.get("last_modified", {}).get("value", None)

    # Handle description (may be dict or str)
    ds = data.get("description")
    if isinstance(ds, dict):
        record["description"] = ds.get("value", "")
    elif isinstance(ds, str):
        record["description"] = ds

    # Handle first_sentence (may be dict or str)
    fs = data.get("first_sentence")
    if isinstance(fs, dict):
        record["first_sentence"] = fs.get("value", "")
    elif isinstance(fs, str):
        record["first_sentence"] = fs

    # If first_sentence still empty, try excerpts
    for excerpt in data.get("excerpts", []):
        text = excerpt.get("text")
        if text and not record["first_sentence"]:
            record["first_sentence"] = text
            break

    return record


# ─── Step 5: Process all ISBNs and compute answers ───────────────────────────────
def compute_answers(records):
    """
    Build a DataFrame, compute answers to questions 1–12,
    and return them in a structured dict.
    """
    df = pd.DataFrame(records)

    # Convert date strings to datetime
    df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")
    df["last_modified"] = pd.to_datetime(df["last_modified"], errors="coerce")

    df.to_csv("books.csv", index=False)

    # Remove any duplicate ISBN rows
    df = df.drop_duplicates(subset=["isbn"])

    answers = {}

    # Q1
    answers[1] = df["title"].nunique()

    # Q2
    title_counts = df.groupby("title")["isbn"].count()
    top_title = title_counts.idxmax()
    top_count = title_counts.max()
    answers[2] = (top_title, top_count)

    # Q3
    answers[3] = df[df["goodreads_ids"].map(len) == 0].shape[0]

    # Q4
    answers[4] = df[df["authors"].map(len) > 1].shape[0]

    # Q5
    publisher_counts = (
        df.explode("publishers")
        .groupby("publishers")
        .size()
        .sort_values(ascending=False)
    )
    answers[5] = publisher_counts.to_dict()

    # Q6
    answers[6] = df["number_of_pages"].dropna().median()

    # Q7
    month_counts = df["publish_date"].dt.month.value_counts()
    if not month_counts.empty:
        best_month_num = int(month_counts.idxmax())
        best_month_name = datetime(1900, best_month_num, 1).strftime("%B")
        answers[7] = (best_month_name, int(month_counts.max()))
    else:
        answers[7] = (None, 0)

    # Q8
    combined_text = (
        df["description"].fillna("") + " " + df["first_sentence"].fillna("")
    ).astype(str)
    stripped = combined_text.apply(lambda txt: re.sub(r"[^\w\s]", "", txt))
    words = stripped.str.split().explode().dropna().unique().tolist()
    longest_len = max((len(w) for w in words), default=0)
    longest_words = [w for w in words if len(w) == longest_len]
    if longest_words:
        pattern = "|".join(re.escape(w) for w in longest_words)
        titles_with = (
            df[
                df["description"].str.contains(pattern, na=False)
                | df["first_sentence"].str.contains(pattern, na=False)
            ]["title"]
            .dropna()
            .unique()
            .tolist()
        )
    else:
        titles_with = []
    answers[8] = {"length": longest_len, "words": longest_words, "titles": titles_with}

    # Q9
    # השלב הראשון - להוריד ספרים שאין להם publish_date תקני
    valid_dates_df = df.dropna(subset=["publish_date"])

    # אם אין אף ספר עם תאריך תקני
    if valid_dates_df.empty:
        answers[9] = (None, None)
    else:
        # נסה לסדר לפי publish_date בצורה יורדת
        valid_dates_df = valid_dates_df.sort_values("publish_date", ascending=False)

        # קח את השורה הראשונה אחרי המיון
        latest_book = valid_dates_df.iloc[0]

        answers[9] = (latest_book["title"], latest_book["publish_date"].date())

    # Q10
    answers[10] = int(df["last_modified"].dt.year.value_counts().idxmax())

    # Q11
    author_title_counts = df.explode("authors").groupby("authors")["title"].nunique()
    if not author_title_counts.empty:
        top_author = author_title_counts.idxmax()
        author_books = df[
            df["authors"].apply(lambda lst: top_author in lst)
        ].sort_values("publish_date")
        answers[11] = author_books.iloc[1]["title"] if len(author_books) > 1 else None
    else:
        answers[11] = None

    # Q12
    pair_counts = (
        df.explode("publishers")
        .explode("authors")
        .groupby(["publishers", "authors"])
        .size()
    )
    if not pair_counts.empty:
        top_pair = pair_counts.idxmax()
        top_pair_count = int(pair_counts.max())
        answers[12] = (top_pair, top_pair_count)
    else:
        answers[12] = (None, 0)

    return answers


# ─── Step 6: Write answers to file ────────────────────────────────────────────────
def save_answers(answers, path):
    """
    Write the answers dict to a text file in a human-readable format.
    """
    with open(path, "w", encoding="utf-8") as f:

        def writeln(*parts):
            print(*parts, file=f)

        writeln("1. Number of different books:", answers[1])
        writeln("2. Book with most ISBNs:", answers[2][0], f"({answers[2][1]} ISBNs)")
        writeln("3. Books without Goodreads ID:", answers[3])
        writeln("4. Books with more than one author:", answers[4])
        writeln("5. Books per publisher:")
        for publisher, count in answers[5].items():
            writeln(f"   - {publisher}: {count}")
        writeln("6. Median number of pages:", answers[6])
        writeln(
            "7. Month with most published books:",
            answers[7][0],
            f"({answers[7][1]} books)",
        )
        writeln(
            "8. Longest word length:",
            answers[8]["length"],
            "Words:",
            answers[8]["words"],
        )
        writeln("   Appears in titles:", answers[8]["titles"])
        writeln("9. Most recently published book:", answers[9][0], "-", answers[9][1])
        writeln("10. Year of most updated entry:", answers[10])
        writeln("11. Second book for top author:", answers[11])
        writeln(
            "12. Top (publisher, author) pair:",
            answers[12][0],
            f"({answers[12][1]} books)",
        )

    # Also print to console
    # with open(path, "r", encoding="utf-8") as f:
    #     print(f.read())


# ─── Main entry point ─────────────────────────────────────────────────────────────
def main():
    # Load data
    isbns = load_isbns(ISBN_FILE)
    cache = load_cache(CACHE_FILE)

    # Fetch and normalize
    records = []
    for isbn in isbns:
        raw = fetch_book_data(isbn, cache)
        records.append(flatten_record(isbn, raw))

    # Persist updated cache
    save_cache(cache, CACHE_FILE)

    # Compute answers
    answers = compute_answers(records)

    # Save and display answers
    save_answers(answers, OUTPUT_FILE)


if __name__ == "__main__":
    main()
