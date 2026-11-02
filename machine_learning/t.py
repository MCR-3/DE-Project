import os
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

import numpy as np
import pandas as pd
from datasets import load_dataset
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix

dataset = load_dataset('hsuvaskakoty/hoaxpedia', 'fulltext_1h2r')
print(f"Dataset: {dataset}") # are there already train/test splits?
df = pd.DataFrame(dataset['train'])
print(f"Columns: {df.columns.tolist()}") # is there a title / page id column?
print(f"Label distribution: {df['label'].value_counts()}") # how imbalanced is it?

X_train, X_test, y_train, y_test = train_test_split(
    df['text'], df['label'], test_size=0.20, random_state=42, stratify=df['label']
)

vectorizer = TfidfVectorizer(max_features=5000, stop_words='english')
X_train_vec = vectorizer.fit_transform(X_train)
X_test_vec = vectorizer.transform(X_test)

model = LogisticRegression(class_weight='balanced', max_iter=1000)
model.fit(X_train_vec, y_train)

y_pred = model.predict(X_test_vec)
print(classification_report(y_test, y_pred, target_names=['Legitimate (0)', 'Hoax (1)']))
print(confusion_matrix(y_test, y_pred))

# Leakage check: which words push toward "hoax"?
words = np.array(vectorizer.get_feature_names_out())
top = np.argsort(model.coef_[0])[-25:]
print("Top hoax-indicating words:", list(words[top]))