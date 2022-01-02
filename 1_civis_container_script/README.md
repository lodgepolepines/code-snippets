Civis Docker container script for auto-uploading ActionBuilder imports from a SurveyGizmo form.

local.py pulls recent data from SurveyGizmo sign-up and, if there are new sign-ups, creates and uploads CSVs to ActionBuilder three imports: 1. Entity import, 2. Info import, and 3. Twitter import (as you can't upload multiple tags to the same field - in this case Social Media).

config.yml files specify coordinates for mapping in ActionBuilder.

upload.py specifies functions used for uploading to ActionBuilder.

Run in container script by installing packages, CD to /app, and run local.py
