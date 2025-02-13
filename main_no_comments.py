from flask import Flask, render_template, request, redirect, url_for, session
import pandas as pd
import random
import os

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.urandom(24)  # Securely generate a random secret key

# Load the CSV file
data = pd.read_csv('Pytania_Testowe.csv')

# Convert to a list of dictionaries for easier processing
questions = []
for index, row in data.iterrows():
    options = [
        row['Odpowiedź poprawna'],
        row['Odpowiedź błędna 1'],
        row['Odpowiedź błędna 2']
    ]
    # Handle optional 'Odpowiedź błędna 3' if it exists and is not NaN
    if 'Odpowiedź błędna 3' in row and pd.notna(row['Odpowiedź błędna 3']):
        options.append(row['Odpowiedź błędna 3'])
    random.shuffle(options)  # Shuffle the options for randomness
    question = {
        'id': index,  # Unique identifier for each question
        'question': row['Pytanie'],
        'correct': row['Odpowiedź poprawna'],
        'options': options
    }
    questions.append(question)


@app.route('/')
def index():
    # Reset session variables when user returns to the home page
    session['score'] = 0
    session['total_attempts'] = 0
    session['incorrect_questions'] = []
    session['remaining_questions'] = list(range(len(questions)))
    random.shuffle(session['remaining_questions'])
    return render_template('index.html')


@app.route('/question')
def question():
    # If no remaining questions, move to the next cycle or show results
    if not session.get('remaining_questions'):
        # If there are incorrect questions from the last cycle
        if session['incorrect_questions']:
            # Prepare for next cycle: set remaining questions to incorrect ones
            session['remaining_questions'] = session['incorrect_questions']
            random.shuffle(session['remaining_questions'])
            session['incorrect_questions'] = []
        else:
            # No incorrect questions left, show final results
            return redirect(url_for('result'))

    # Display the first question from the remaining_questions list
    q_index = session['remaining_questions'][0]
    current_question = questions[q_index]

    # Calculate statistics
    questions_left = len(session['remaining_questions'])
    correct_answers = session.get('score', 0)
    incorrect_answers = len(session['incorrect_questions'])

    return render_template('question.html',
                           question=current_question,
                           question_id=q_index,
                           questions_left=questions_left,
                           correct_answers=correct_answers,
                           incorrect_answers=incorrect_answers)


@app.route('/submit_answer', methods=['POST'])
def submit_answer():
    selected_answer = request.form.get('selected_answer')
    question_id = int(request.form.get('question_id'))
    current_question = questions[question_id]
    correct_answer = current_question['correct']
    question_text = current_question['question']  # Get the question text

    # Check correctness
    is_correct = (selected_answer == correct_answer)
    session['total_attempts'] += 1

    if is_correct:
        session['score'] += 1
    else:
        # If incorrect, add to the incorrect_questions list
        session['incorrect_questions'].append(question_id)

    # Remove this question from remaining_questions
    if question_id in session['remaining_questions']:
        session['remaining_questions'].remove(question_id)

    # Calculate statistics
    questions_left = len(session['remaining_questions'])
    correct_answers = session.get('score', 0)
    incorrect_answers = len(session['incorrect_questions'])

    # Show feedback page with question text
    return render_template('feedback.html',
                           is_correct=is_correct,
                           correct_answer=correct_answer,
                           question_text=question_text,  # Pass question text
                           questions_left=questions_left,
                           correct_answers=correct_answers,
                           incorrect_answers=incorrect_answers)


@app.route('/next')
def next_question():
    # Move to the next question if there are any left
    return redirect(url_for('question'))


@app.route('/result')
def result():
    score = session.get('score', 0)
    total = len(questions)
    total_attempts = session.get('total_attempts', 0)
    incorrect_final = total - score
    return render_template('result.html',
                           score=score,
                           total=total,
                           total_attempts=total_attempts,
                           incorrect_final=incorrect_final)


if __name__ == '__main__':
    app.run(debug=True)
