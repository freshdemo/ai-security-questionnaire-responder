README.md

Request For Information Responder

This tool allows you to upload source of truth documents (security questionnairres, RFP's, RFI's, etc) to Google Gemini, read security requirements from a Google Sheet, use the LLM to compare requirements in the Sheet to what is in the documents, and provide responses in the Google Sheet.


    SETUP INSTRUCTIONS:
    
    1. GET GEMINI API KEY:
       - Go to https://aistudio.google.com/app/apikey
       - If you are on a free plan this will not work well and you want to be on enterprise.
       - Create a new API key
       - From your CLI run 'export GEMINI_API_KEY=<the_key>'. You can also update your ~/.zshrc file with this to make it more permanent.
    
    2. SETUP GOOGLE SHEETS API:
       - Go to https://console.cloud.google.com/
       - Create/select a project
       - Enable Google Sheets API and Google Drive API
       - Create Service Account credentials
       - Navigate to Keys and Add Key
       - Download the JSON key file
       - Share your Google Sheet with the service account email
       - Update the python script SERVICE_ACCOUNT_FILE to point to the json key file
    
    3. GET SPREADSHEET ID:
       - From your Google Sheets URL: 
         https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
       - Copy the SPREADSHEET_ID part
       - Update the script or from the CLI run 'export SPREADSHEET_ID=<your spreadsheet_id>'
    
    4. UPDATE COLUMN NAMES:
       - Make sure your sheet has columns named:
         * 'Requirement'
         * 'Compliance_Statement'
    
    6. DOWNLOAD RELEVANT DOCUMENTS:
       - Download relevant compliance documents. It's best to use the ISO27001, SIG Lite, and SOC2 report. Most of the other ones may generate undesireable responses.
       - Move them into a docs/ folder in this project.

    7. DOWNLOAD AND RUN THE SCRIPT:
       - Get the latest version from the GitHub Releases 'https://github.com/freshdemo/ai-security-questionnaire-responder/releases'.
       - Allow the binary to be run on your Mac 'xattr -d com.apple.quarantine security_questionnaire_responder'. This is required as it's not signed by Apple.
       - Run the script with './security_questionnaire_responder' from the project folder.
       - You should see it uploading the documents to Gemini, and then starting to populate the spreadsheet.

    8. TUNING:
      - Modify prompts for Gemini; They can be found in the prompts folder. Here you can modify how Gemini will be used to generate the responses.
      - The tool is multi-threaded with a default of 4 workers. You can either set 'export GEMINI_MAX_WORKERS=8' to make it run faster.



## License

This project is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.

[![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

**You are free to:**
- Share — copy and redistribute the material in any medium or format
- Adapt — remix, transform, and build upon the material

**Under the following terms:**
- Attribution — You must give appropriate credit
- NonCommercial — You may not use the material for commercial purposes
- ShareAlike — If you remix, transform, or build upon the material, you must distribute your contributions under the same license

See the [LICENSE](LICENSE) file for details.
