#!/usr/bin/env python3
"""
Demo Web Application - Expense Tracker

A simple expense tracking application that demonstrates
MiniDB's CRUD operations and JOIN capabilities.

Features:
- Create categories and expenses
- Track spending by category
- View expense history with category names (using JOINs)
- Dashboard with aggregated statistics

Run:
    pip install flask
    python app.py
    
Then visit: http://localhost:5000
"""

import os
import sys
from datetime import datetime, date
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

# Add parent directory to path to import minidb
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from minidb import Database

app = Flask(__name__)
app.secret_key = 'minidb-demo-secret-key-change-in-production'

# Initialize database
DATA_DIR = os.path.join(os.path.dirname(__file__), 'expense_data')
db = Database(DATA_DIR)


def init_database():
    """Initialize database tables if they don't exist."""
    # Create categories table
    if 'categories' not in db.tables():
        db.execute("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL UNIQUE,
                color VARCHAR(7) DEFAULT '#6366f1',
                icon VARCHAR(50) DEFAULT 'folder'
            )
        """)
        
        # Insert default categories
        default_categories = [
            (1, 'Food & Dining', '#ef4444', 'utensils'),
            (2, 'Transportation', '#f97316', 'car'),
            (3, 'Shopping', '#eab308', 'shopping-bag'),
            (4, 'Entertainment', '#22c55e', 'film'),
            (5, 'Bills & Utilities', '#3b82f6', 'file-text'),
            (6, 'Healthcare', '#ec4899', 'heart'),
            (7, 'Other', '#6b7280', 'more-horizontal'),
        ]
        for cat in default_categories:
            db.execute(f"INSERT INTO categories VALUES ({cat[0]}, '{cat[1]}', '{cat[2]}', '{cat[3]}')")
    
    # Create expenses table
    if 'expenses' not in db.tables():
        db.execute("""
            CREATE TABLE expenses (
                id INTEGER PRIMARY KEY,
                description VARCHAR(255) NOT NULL,
                amount FLOAT NOT NULL,
                category_id INTEGER NOT NULL,
                expense_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT '2024-01-01 00:00:00'
            )
        """)
        
        # Create index on category_id for faster JOINs
        db.execute("CREATE INDEX idx_expenses_category ON expenses(category_id)")
        db.execute("CREATE INDEX idx_expenses_date ON expenses(expense_date)")


# Initialize database on startup
init_database()


@app.route('/')
def index():
    """Dashboard with expense overview."""
    # Get total expenses
    total_result = db.execute("SELECT SUM(amount) as total FROM expenses")
    total = total_result.rows[0]['total'] if total_result.rows[0]['total'] else 0
    
    # Get expense count
    count_result = db.execute("SELECT COUNT(*) as cnt FROM expenses")
    count = count_result.rows[0]['cnt']
    
    # Get expenses by category (using JOIN)
    by_category = db.execute("""
        SELECT c.name, c.color, SUM(e.amount) as total
        FROM expenses e
        JOIN categories c ON e.category_id = c.id
        GROUP BY c.name, c.color
        ORDER BY total DESC
    """)
    
    # Get recent expenses (using JOIN)
    recent = db.execute("""
        SELECT e.id, e.description, e.amount, e.expense_date, c.name as category, c.color
        FROM expenses e
        JOIN categories c ON e.category_id = c.id
        ORDER BY e.expense_date DESC
        LIMIT 5
    """)
    
    return render_template('index.html',
                          total=total,
                          count=count,
                          by_category=by_category.rows,
                          recent=recent.rows)


@app.route('/expenses')
def expenses():
    """List all expenses."""
    # Get all expenses with category names (JOIN)
    result = db.execute("""
        SELECT e.id, e.description, e.amount, e.expense_date, 
               c.name as category, c.color, c.icon
        FROM expenses e
        JOIN categories c ON e.category_id = c.id
        ORDER BY e.expense_date DESC, e.id DESC
    """)
    
    return render_template('expenses.html', expenses=result.rows)


@app.route('/expenses/add', methods=['GET', 'POST'])
def add_expense():
    """Add a new expense."""
    if request.method == 'POST':
        description = request.form.get('description', '').replace("'", "''")
        amount = float(request.form.get('amount', 0))
        category_id = int(request.form.get('category_id', 1))
        expense_date = request.form.get('expense_date', date.today().isoformat())
        
        if not description or amount <= 0:
            flash('Please provide a valid description and amount.', 'error')
            return redirect(url_for('add_expense'))
        
        # Get next ID
        id_result = db.execute("SELECT MAX(id) as max_id FROM expenses")
        next_id = (id_result.rows[0]['max_id'] or 0) + 1
        
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        db.execute(f"""
            INSERT INTO expenses VALUES (
                {next_id}, 
                '{description}', 
                {amount}, 
                {category_id}, 
                '{expense_date}',
                '{created_at}'
            )
        """)
        
        flash('Expense added successfully!', 'success')
        return redirect(url_for('expenses'))
    
    # GET - show form
    categories = db.execute("SELECT * FROM categories ORDER BY name")
    return render_template('add_expense.html', categories=categories.rows)


@app.route('/expenses/<int:expense_id>/edit', methods=['GET', 'POST'])
def edit_expense(expense_id):
    """Edit an expense."""
    if request.method == 'POST':
        description = request.form.get('description', '').replace("'", "''")
        amount = float(request.form.get('amount', 0))
        category_id = int(request.form.get('category_id', 1))
        expense_date = request.form.get('expense_date')
        
        db.execute(f"""
            UPDATE expenses 
            SET description = '{description}',
                amount = {amount},
                category_id = {category_id},
                expense_date = '{expense_date}'
            WHERE id = {expense_id}
        """)
        
        flash('Expense updated successfully!', 'success')
        return redirect(url_for('expenses'))
    
    # GET - show form
    expense = db.execute(f"SELECT * FROM expenses WHERE id = {expense_id}")
    if not expense.rows:
        flash('Expense not found.', 'error')
        return redirect(url_for('expenses'))
    
    categories = db.execute("SELECT * FROM categories ORDER BY name")
    return render_template('edit_expense.html', 
                          expense=expense.rows[0], 
                          categories=categories.rows)


@app.route('/expenses/<int:expense_id>/delete', methods=['POST'])
def delete_expense(expense_id):
    """Delete an expense."""
    db.execute(f"DELETE FROM expenses WHERE id = {expense_id}")
    flash('Expense deleted successfully!', 'success')
    return redirect(url_for('expenses'))


@app.route('/categories')
def categories():
    """List all categories with expense counts."""
    result = db.execute("""
        SELECT c.id, c.name, c.color, c.icon, COUNT(e.id) as num_expenses
        FROM categories c
        LEFT JOIN expenses e ON c.id = e.category_id
        GROUP BY c.id, c.name, c.color, c.icon
        ORDER BY c.name
    """)
    
    return render_template('categories.html', categories=result.rows)


@app.route('/categories/add', methods=['GET', 'POST'])
def add_category():
    """Add a new category."""
    if request.method == 'POST':
        name = request.form.get('name', '').replace("'", "''")
        color = request.form.get('color', '#6366f1')
        icon = request.form.get('icon', 'folder')
        
        if not name:
            flash('Please provide a category name.', 'error')
            return redirect(url_for('add_category'))
        
        # Get next ID
        id_result = db.execute("SELECT MAX(id) as max_id FROM categories")
        next_id = (id_result.rows[0]['max_id'] or 0) + 1
        
        try:
            db.execute(f"INSERT INTO categories VALUES ({next_id}, '{name}', '{color}', '{icon}')")
            flash('Category added successfully!', 'success')
        except Exception as e:
            flash(f'Error: {e}', 'error')
        
        return redirect(url_for('categories'))
    
    return render_template('add_category.html')


@app.route('/categories/<int:category_id>/delete', methods=['POST'])
def delete_category(category_id):
    """Delete a category."""
    # Check if category has expenses
    check = db.execute(f"SELECT COUNT(*) as cnt FROM expenses WHERE category_id = {category_id}")
    if check.rows[0]['cnt'] > 0:
        flash('Cannot delete category with existing expenses.', 'error')
        return redirect(url_for('categories'))
    
    db.execute(f"DELETE FROM categories WHERE id = {category_id}")
    flash('Category deleted successfully!', 'success')
    return redirect(url_for('categories'))


@app.route('/api/stats')
def api_stats():
    """API endpoint for dashboard statistics."""
    # Total expenses
    total = db.execute("SELECT SUM(amount) as total FROM expenses").rows[0]['total'] or 0
    
    # Count
    count = db.execute("SELECT COUNT(*) as cnt FROM expenses").rows[0]['cnt']
    
    # By category
    by_category = db.execute("""
        SELECT c.name, SUM(e.amount) as total
        FROM expenses e
        JOIN categories c ON e.category_id = c.id
        GROUP BY c.name
    """)
    
    return jsonify({
        'total': total,
        'count': count,
        'by_category': {row['name']: row['total'] for row in by_category.rows}
    })


if __name__ == '__main__':
    print("\n" + "="*60)
    print("MiniDB Demo - Expense Tracker")
    print("="*60)
    print(f"\nDatabase location: {DATA_DIR}")
    print("Starting server at http://localhost:5000")
    print("\nPress Ctrl+C to stop the server.\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
