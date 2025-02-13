from flask import Flask, render_template, request
import pandas as pd
import random

# Load the CSV file
data = pd.read_csv('/mnt/data/Pytania_Testowe.csv')

# Convert to a list of dictionaries for easier processing
questions = []
for index, row in data.iterrows():
    question = {
        'question': row['Pytanie'],
        'correct': row['Odpowiedź poprawna'],
        'options': [row['Odpowiedź poprawna'], row['Odpowiedź błędna 1'], row['Odpowiedź błędna 2']]
    }
    random.shuffle(question['options'])  # Shuffle the options for randomness
    questions.append(question)

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html', questions=questions)

@app.route('/submit', methods=['POST'])
def submit():
    correct_count = 0
    total_questions = len(questions)

    for i, question in enumerate(questions):
        selected = request.form.get(f'question-{i}')
        if selected == question['correct']:
            correct_count += 1

    return render_template('result.html', score=correct_count, total=total_questions)

if __name__ == '__main__':
    app.run(debug=True)
