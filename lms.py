import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime, timedelta

# ------------------- Setup -------------------
load_dotenv()
url = os.getenv("supabase_url")
key = os.getenv("supabase_key")
sb: Client = create_client(url, key)

# ------------------- Members -------------------
def add_member(name: str, email: str):
    if not name or not email:
        return {"error": "Name and email are required"}
    existing = sb.table("members").select("*").eq("email", email).execute()
    if existing.data:
        return {"error": "Email already exists"}
    return sb.table("members").insert({"name": name, "email": email}).execute()

def update_member(member_id: int, name: str = None, email: str = None):
    update_data = {}
    if name: update_data["name"] = name
    if email: update_data["email"] = email
    if not update_data: return {"error": "Nothing to update"}
    return sb.table("members").update(update_data).eq("member_id", member_id).execute()

def delete_member(member_id: int):
    borrowed = sb.table("borrow_records").select("*").eq("member_id", member_id).is_("return_date", None).execute()
    if borrowed.data:
        return {"error": "Cannot delete member; they have borrowed books"}
    return sb.table("members").delete().eq("member_id", member_id).execute()

def get_member(member_id: int):
    member = sb.table("members").select("*").eq("member_id", member_id).single().execute()
    borrowed = sb.table("borrow_records").select("*,books(title,author)").eq("member_id", member_id).is_("return_date", None).execute()
    return member, borrowed

# ------------------- Books -------------------
def add_book(title: str, author: str, category: str, stock: int):
    if not title or not author or stock < 0:
        return {"error": "Invalid book data"}
    return sb.table("books").insert({"title": title, "author": author, "category": category, "stock": stock}).execute()

def list_books():
    return sb.table("books").select("*").execute()

def search_book(term: str):
    if not term:
        return {"error": "Search term required"}
    return sb.table("books").select("*").or_(
        [f"title.ilike.%{term}%", f"author.ilike.%{term}%", f"category.ilike.%{term}%"]
    ).execute()

def update_stock(book_id: int, new_stock: int):
    if new_stock < 0:
        return {"error": "Stock cannot be negative"}
    return sb.table("books").update({"stock": new_stock}).eq("book_id", book_id).execute()

def delete_book(book_id: int):
    borrowed = sb.table("borrow_records").select("*").eq("book_id", book_id).is_("return_date", None).execute()
    if borrowed.data:
        return {"error": "Book is currently borrowed"}
    return sb.table("books").delete().eq("book_id", book_id).execute()

# ------------------- Borrow/Return -------------------
def borrow_book(member_id: int, book_id: int):
    book = sb.table("books").select("stock").eq("book_id", book_id).single().execute()
    if not book.data:
        return {"error": "Book not found"}
    if book.data["stock"] <= 0:
        return {"error": "Book not available"}
    sb.table("books").update({"stock": book.data["stock"] - 1}).eq("book_id", book_id).execute()
    return sb.table("borrow_records").insert({"member_id": member_id, "book_id": book_id, "borrow_date": datetime.utcnow().replace(microsecond=0).isoformat()}).execute()

def return_book(record_id: int):
    record = sb.table("borrow_records").select("*").eq("record_id", record_id).single().execute()
    if not record.data or record.data["return_date"]:
        return {"error": "Invalid or already returned record"}
    book_id = record.data["book_id"]
    now = datetime.utcnow().replace(microsecond=0).isoformat()
    sb.table("borrow_records").update({"return_date": now}).eq("record_id", record_id).execute()
    book = sb.table("books").select("stock").eq("book_id", book_id).single().execute()
    return sb.table("books").update({"stock": book.data["stock"] + 1}).eq("book_id", book_id).execute()

# ------------------- Reports -------------------
def top_books(n=5):
    return sb.table("borrow_records").select("book_id, books(title, author), count(*)").group("book_id").order("count", desc=True).limit(n).execute()

def overdue_members(days=14):
    cutoff = datetime.utcnow() - timedelta(days=days)
    return sb.table("borrow_records").select("member_id, books(title)").is_("return_date", None).lt("borrow_date", cutoff.isoformat()).execute()

def borrowed_count_per_member():
    return sb.table("borrow_records").select("member_id, count(*)").group("member_id").execute()

# ------------------- Testing -------------------
if __name__ == "__main__":
    print(add_member("Bhavani","bhava@gmail.com"))
    print(add_book("Computer Networks","Frozen","Self-help",4))
    print(list_books().data)
    print(update_stock(1, 10))
    print(borrow_book(1, 1))
    print(return_book(1))
    print(top_books(5).data)
    print(overdue_members(14).data)
    print(borrowed_count_per_member().data)
