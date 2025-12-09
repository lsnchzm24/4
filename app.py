from flask import Flask, render_template, jsonify, request, redirect, url_for, session, flash
from flask_session import Session
from functools import wraps
import pandas as pd
import pyodbc
from datetime import datetime, date, timedelta
from werkzeug.utils import secure_filename
from flask import send_from_directory
import json
import logging
import uuid
import os




app = Flask(__name__)

# REQUIRED: Set secret key for sessions
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or 'dev-key-change-in-production-12345'

# Optional: Configure session settings
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True

# Initialize session
Session(app)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def require_incidents_access(f):
    """Decorator to restrict access to incidents section for specific users"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        
        # Check if user has incidents access
        user_id = session.get('userID')
        allowed_users = [108, 111, 103]  # Users allowed to access incidents
        
        if user_id not in allowed_users:
            flash('Access denied. You do not have permission to view incidents.', 'error')
            return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function




def require_ci_access(f):
    """Decorator to restrict access to incidents section for specific users"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        
        # Check if user has incidents access
        user_id = session.get('userID') 
        allowed_users = [131, 103, 42] 
        
        if user_id not in allowed_users:
            flash('Access denied. You do not have permission to view incidents.', 'error')
            return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function

def require_logistics_access(f):
    """Decorator to restrict access to incidents section for specific users"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        
        # Check if user has incidents access
        user_id = session.get('userID')
        allowed_users = [166, 103, 160, 161, 185, 106, 52]  # Users allowed to access incidents
        
        if user_id not in allowed_users:
            flash('Access denied. You do not have permission to view incidents.', 'error')
            return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function


def require_quality_access(f):
    """Decorator to restrict access to incidents section for specific users"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        
        # Check if user has incidents access
        user_id = session.get('userID')
        allowed_users = [103, 110, 97, 96, 91, 40]  # Users allowed to access quality
        
        if user_id not in allowed_users:
            flash('Access denied. You do not have permission to view incidents.', 'error')
            return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function

def require_production_access(f):
    """Decorator to restrict access to incidents section for specific users"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        
        # Check if user has incidents access
        user_id = session.get('userID')
        allowed_users = [103, 184, 11, 12, 106, 52, 94, 17, 94, 178, 159] 
        
        if user_id not in allowed_users:
            flash('Access denied. You do not have permission to view incidents.', 'error')
            return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function

# Get the directory where app.py is located
APP_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(APP_DIR, 'daily_photos')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH



def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def RPT_Operation(ID_QA):
    db_connection_string = get_db_connection()
    crsr = db_connection_string.cursor()
    operation =crsr.execute("SELECT Machine, Instruction FROM QualityAlert_Operations WHERE ID_QA = ?", ID_QA).fetchall()
    crsr.close() 
    db_connection_string.close()
    return operation

def RPT_Material(ID_QA):
    db_connection_string = get_db_connection()
    crsr = db_connection_string.cursor()
    operation =crsr.execute("  SELECT A.SAPNumber, A.BMWNumber, B.Description FROM QualityAlert_Material A INNER JOIN Quality_Material B ON(A.SAPNumber = B.BentelerNumber)  WHERE A.ID_QA = ?", ID_QA).fetchall()
    crsr.close() 
    db_connection_string.close()
    return operation

def RPT_Main(ID_QA):
    db_connection_string = get_db_connection()
    crsr = db_connection_string.cursor()
    operation =crsr.execute("SELECT A.ID_QA, C.name, A.WhoFound, A.Where1, A.Where2, FORMAT(A.WhenF, 'dddd') as WhenF, A.Line, A.Defect, A.QANumber, FORMAT(A.FromDate, 'MMMM-dd-yyyy') as FromDate, A.Status, A.Quantity, A.IssueReportedBy, B.shift, A.OkIMG, A.NokIMG, A.WasWere, FORMAT(A.WhenF, 'M') as Month, FORMAT(A.WhenF, 'yyyy') as Year, A.QualityAuthorization, A.ProductionAuthorization, FORMAT(A.FechaFinal, 'MMMM-dd-yyyy'), A.CleanPointNRO FROM QualityAlert_Main A INNER JOIN Shift B ON (A.shiftID = B.shiftID) INNER JOIN Users C ON(A.CreatedBy = C.userID) WHERE A.ID_QA = ?", ID_QA).fetchall()
    crsr.close() 
    db_connection_string.close()
    return operation


# Add these imports to your app.py (if not already present)
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Department email configuration - add to your app.py
# Meeting Actions Email Configuration - add to your app.py (reusing your existing 6S email setup)
# This uses the same SMTP configuration and approach as your 6S audit system

MEETING_ACTIONS_DEPARTMENT_CONFIG = {
                                     
    "CIP/Training": {
        "emails": ["John.Risher@benteler.com", "Tammy.Billings@benteler.com", "luis.sanchez@benteler.com"],
        "subject_prefix": "CIP/Training - Meeting Action Item: "
    },

    "Controls": {
        "emails": ["David.McCowan@benteler.com", "Evanildo.Maier@benteler.com", "Brett.Kitchens@benteler.com", "Christian.Kiwitz@benteler.com"],
        "subject_prefix": "Controls Team - Meeting Action Item: "
    },
    "Maintenance": {
        "emails": ["joshua.smith@benteler.com", "Jeff.Mabry@benteler.com", "Chris.Upchurch@benteler.com", "luis.sanchez@benteler.com", "koua.her@benteler.com", "jhon.montoyaruiz@benteler.com", "nhia.moua@benteler.com", "joel.white@benteler.com", "ron.chimm@benteler.com", "david.bishop@benteler.com", "frank.tealiiitrace@benteler.com", "jorge.ardilasanchez@benteler.com", "james.ellisoniii@benteler.com", "randy.sanchez@benteler.com", "bret.harvey@benteler.com", "christopher.allen-gadson@benteler.com", "kou.moua1@benteler.com", "jimmy.waters@benteler.com" ],
        "subject_prefix": "Maintenance Team - Meeting Action Item: "
    },
    "IT (Hardware)": { 
        "emails": ["Chris.Schultz@benteler.com", "itzel.johnson@benteler.com", "luis.sanchez@benteler.com"],
        "subject_prefix": "IT Support Team - Meeting Action Item: "
    },
    "IT (Software)": { 
        "emails": [ "luis.sanchez@benteler.com"],
        "subject_prefix": "IT Support Team - Meeting Action Item: "
    },
    "Engineering": {
        "emails": [ "roberto.bertossi1@benteler.com", "Giovanni.Rivera@benteler.com", "Oscar.ReyesPena@benteler.com" ],
        "subject_prefix": "Engineering Team - Meeting Action Item: "
    },

    "Logistics": {
        "emails": ["luis.sanchez@benteler.com", "colin.barth@benteler.com", "Jason.Kuhn@benteler.com"],
        "subject_prefix": "Logistics Team - Meeting Action Item: "
    },
    "Operations": {
        "emails": ["derek.watson@benteler.com", "kenneth.smithiii@benteler.com", "jacob.wasserman@benteler.com" ,"amber.lamb@benteler.com", "luis.sanchez@benteler.com"],
        "subject_prefix": "Operations Team - Meeting Action Item: "
    },
    "Quality": {
        "emails": ["brandon.howe@benteler.com", "juan.loaiza@benteler.com", "gareth.mclaughlin@benteler.com", "jesus.palacios@benteler.com", "sarisha.douglas1@benteler.com"],
        "subject_prefix": "Quality Team - Meeting Action Item: "
    },
    "Safety": {
        "emails": ["amber.lamb@benteler.com", "jessica.walker@benteler.com"],
        "subject_prefix": "Safety Team - Meeting Action Item: "
    }
    
    
}

# Email configuration - same as your existing one
MEETING_EMAIL_CONFIG = {
    "smtp_server": "172.25.8.104",
    "smtp_port": 25
}

# Meeting Actions Email Function - based on your send_team_notification function
def send_meeting_action_notification(assignee, actions, department=None, meeting_date=None, session_id=None):
    """
    Send automatic email notification for meeting action items - works like your 6S system
    """
    try:
        # Determine recipient emails based on department or individual
        if department and department in MEETING_ACTIONS_DEPARTMENT_CONFIG:
            # Send to department team (like your 6S system)
            dept_config = MEETING_ACTIONS_DEPARTMENT_CONFIG[department]
            recipients = dept_config["emails"]
            subject_prefix = dept_config["subject_prefix"]
            recipient_name = f"{department} Team"
        else:
            # Send to individual assignee (estimate email from name)
            # This follows the pattern from your system but for individuals
            assignee_email = f"{assignee.lower().replace(' ', '.')}@benteler.com"
            recipients = [assignee_email]
            subject_prefix = "Meeting Action Item: "
            recipient_name = assignee
        
        # Format meeting date (same as your 6S system)
        if meeting_date:
            try:
                formatted_date = datetime.fromisoformat(meeting_date.replace('Z', '+00:00')).strftime('%B %d, %Y')
            except:
                formatted_date = datetime.now().strftime('%B %d, %Y')
        else:
            formatted_date = datetime.now().strftime('%B %d, %Y')
        
        # Create subject (same pattern as your 6S system)
        subject = f"{subject_prefix}Action Items from {formatted_date} Meeting"
        
        # Sender email (same as your 6S system)
        sender_email = "Daily OpEx System <noreply@benteler.com>"
        
        # Use your existing SMTP configuration
        smtp_server = "172.25.8.104"
        smtp_port = 25
        
        # Create email message
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = ', '.join(recipients)
        msg["Subject"] = subject
        
        # Add sender to CC (same as your 6S system)
        receiver_CC = []
        if session.get('Email'):
            receiver_CC.append(session.get('Email'))
        
        # Create HTML content (similar style to your 6S emails)
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ padding: 0; max-width: 700px; margin: 0 auto; }}
                .header {{ background-color: #3f567a; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; background-color: #f9f9f9; }}
                .action-item {{ margin: 15px 0; padding: 15px; background-color: white; 
                              border-left: 4px solid #3f567a; border-radius: 4px; }}
                .priority-high {{ border-left-color: #dc3545; }}
                .priority-medium {{ border-left-color: #ffc107; }}
                .priority-low {{ border-left-color: #28a745; }}
                .footer {{ text-align: center; margin-top: 20px; font-size: 0.9em; 
                          color: #6c757d; padding: 10px; }}
                .button {{ display: inline-block; background-color: #3f567a; color: white; 
                          padding: 10px 20px; text-decoration: none; border-radius: 4px; 
                          margin-top: 15px; }}
                .meta-info {{ font-size: 0.9em; color: #666; margin-top: 10px; }}
                .due-date {{ font-weight: bold; color: #dc3545; }}
                .department-badge {{ display: inline-block; background-color: #e9ecef; 
                                   color: #495057; padding: 2px 8px; border-radius: 3px; 
                                   font-size: 0.8em; margin-left: 10px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2 style="margin: 0;">Daily OpEx Meeting Actions</h2>
                    <p style="margin: 5px 0 0 0;">Assigned to: {recipient_name}</p>
                </div>
                <div class="content">
                    <p>Dear {recipient_name},</p>
                    
                    <p>You have been assigned the following action items from the meeting on {formatted_date}:</p>
        """


        # Add each action item (similar to your 6S details section)
        for index, action in enumerate(actions, 1):
            priority_class = f"priority-{action.get('priority', 'medium').lower()}"
            due_date_text = ""
            
            if action.get('dueDate'):
                try:
                    due_date = datetime.fromisoformat(action['dueDate'].replace('Z', '+00:00'))
                    due_date_text = f"<div class='due-date'>Due Date: {due_date.strftime('%B %d, %Y')}</div>"
                except:
                    due_date_text = f"<div class='due-date'>Due Date: {action.get('dueDate')}</div>"
            
            source_data = action.get('sourceData', {})
            action_type = source_data.get('type', 'Action Item')
            description = source_data.get('description', 'No description available')
            
            html_content += f"""
                    <div class="action-item {priority_class}">
                        <h4 style="margin-top: 0;">Action Item #{index}: {action_type}</h4>
                        <p><strong>Description:</strong></p>
                        <p>{description}</p>
                        {due_date_text}
                        <div class="meta-info">
                            <strong>Department:</strong> {action.get('department', 'Not specified')}<br>
                            <strong>Priority:</strong> {action.get('priority', 'Medium')}<br>
                            <strong>Status:</strong> {action.get('status', 'Pending')}
                            {f"<br><strong>Notes:</strong> {action.get('notes')}" if action.get('notes') else ""}
                        </div>
                    </div>
            """
        
        # Complete HTML (same footer style as your 6S system)
        html_content += f"""
                    <p>Please review these action items and take appropriate action by the specified due dates.</p>
                    
                    
                <div class="footer">
                    <p>This is an automated notification from the Daily OpEx System. Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML content
        msg.attach(MIMEText(html_content, "html"))
        
        # Send email using the SAME method as your 6S system
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            # No authentication or TLS - exactly like your working 6S code
            server.sendmail(sender_email, recipients + receiver_CC, msg.as_string())
            
        print(f"Meeting action email sent to {recipient_name}: {', '.join(recipients)}")
        return True
        
    except Exception as e:
        print(f"Error sending meeting action email to {assignee}: {e}")
        return False


    # Global variable for MRB general note
    mrb_general_note = ""

def load_mrb_general_note():
        """Load MRB general note from file"""
        global mrb_general_note
        try:
            import json
            with open('mrb_general_note.json', 'r') as f:
                data = json.load(f)
                mrb_general_note = data.get('note', '')
        except FileNotFoundError:
            mrb_general_note = ""

def save_mrb_general_note(note):
        """Save MRB general note to file"""
        global mrb_general_note
        import json
        mrb_general_note = note
        with open('mrb_general_note.json', 'w') as f:
            json.dump({
                'note': note,
                'updated_at': datetime.now().isoformat(),
                'updated_by': session.get('name', 'System')
            }, f)

    # Call when app starts (add this near load_selected_scraps() and load_selected_mrb())
load_mrb_general_note()




@app.route('/api/meeting-actions/send-individual-email', methods=['POST'])
def send_individual_meeting_action_email():
    """
    API endpoint to send individual email notification for a single meeting action item
    """
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        email_data = request.get_json()
        
        # Extract data for single action
        action = email_data.get('action', {})
        assignee = email_data.get('assignee')
        meeting_date = email_data.get('meetingDate')
        session_id = email_data.get('sessionId')
        action_index = email_data.get('actionIndex', 1)
        total_actions = email_data.get('totalActions', 1)
        
        if not assignee or not action:
            return jsonify({'error': 'Missing assignee or action data'}), 400
        
        # Get department from action
        department = action.get('department')
        
        # Send individual email using your existing function
        if send_meeting_action_notification(
            assignee=assignee,
            actions=[action],  # Send as single-item array
            department=department,
            meeting_date=meeting_date,
            session_id=session_id
        ):
            # Mark this specific action as email sent in database
            try:
                if action.get('actionID'):
                    update_query = """
                    UPDATE dbo.DailyActions SET EmailSent = 1 
                    WHERE ActionID = ?
                    """
                    conn = get_db_connection_1()
                    cursor = conn.cursor()
                    cursor.execute(update_query, [action['actionID']])
                    conn.commit()
                    cursor.close()
                    conn.close()
            except Exception as db_error:
                logger.warning(f"Could not update EmailSent flag: {db_error}")
            
            return jsonify({
                'success': True,
                'message': f'Email sent successfully to {assignee} for action {action_index} of {total_actions}'
            })
        else:
            return jsonify({
                'error': f'Failed to send email to {assignee}'
            }), 500
            
    except Exception as e:
        logger.error(f"Error in send individual meeting action email: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

from email.mime.base import MIMEBase
from email import encoders

@app.route('/api/meeting-actions/send-completion-email/<int:action_id>', methods=['POST'])
def send_meeting_completion_email(action_id):
    """
    Send email with existing PDF attached when meeting is completed
    """
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        # Get current user info
        completed_by = session.get('name', 'System')
        
        # Send the email with existing PDF attached
        success = send_completion_email_with_existing_pdf(completed_by)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Meeting completion notification sent successfully with PDF attached'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to send notification - PDF not found or could not be attached. Please ensure the PDF has been generated before completing the meeting.'
            }), 500
            
    except Exception as e:
        logger.error(f"Error sending completion email: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': 'An unexpected error occurred while sending the completion email',
            'details': str(e)
        }), 500

from datetime import datetime, time

def send_completion_email_with_existing_pdf(completed_by):
    """
    Send email notification with existing PDF from network folder attached
    """
    try:
        # Email configuration
        recipients = ["steven.conner@benteler.com","jacob.wasserman@benteler.com", "brandon.howe@benteler.com"]
        cc_recipients = ["luis.sanchez@benteler.com", "John.Risher@benteler.com"]
        sender_email = "Daily OpEx System <noreply@benteler.com>"
        smtp_server = "172.25.8.104"
        smtp_port = 25

        # Time and depth calculation
        now = datetime.now()
        meeting_start = datetime.combine(now.date(), time(9, 0))
        duration_minutes = (now - meeting_start).total_seconds() / 60

        if duration_minutes < 19:
            depth_label = "Weak"
            depth_color = "#dc3545"  
        elif 19 <= duration_minutes <= 29:
            depth_label = "Acceptable"
            depth_color = "#fd7e14"  
        else:
            depth_label = "Good"
            depth_color = "#28a745"  

        current_date = now.strftime('%A, %B %d, %Y')
        subject = f"Daily OpEx Meeting Completed - {current_date}"

        # Create email message
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = ', '.join(recipients)
        msg["Cc"] = ', '.join(cc_recipients)
        msg["Subject"] = subject

        # HTML content
        html_content = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: 'Segoe UI', Arial, sans-serif;
                    background-color: #f4f6f8;
                    margin: 0;
                    padding: 0;
                    color: #333;
                }}
                .container {{
                    max-width: 700px;
                    margin: 10px auto;
                    background-color: #ffffff;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #3f567a 0%, #2d4059 100%);
                    color: #ffffff;
                    padding: 20px 20px 30px 20px;
                    text-align: center;
                }}
                .checkmark {{
                    font-size: 50px;
                    color: #28a745;
                    margin-bottom: 1px;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 1.8em;
                }}
                .header p {{
                    margin: 10px 0 0;
                    font-size: 1.1em;
                    font-weight: 300;
                }}
                .content {{
                    padding: 30px 20px;
                }}
                .summary-box {{
                    background-color: #f9f9f9;
                    border-left: 5px solid #3f567a;
                    padding: 20px;
                    border-radius: 6px;
                    margin-bottom: 20px;
                }}
                .summary-box h2 {{
                    margin-top: 0;
                    color: #2d4059;
                    font-size: 1.4em;
                }}
                .depth-box {{
                    background-color: #f9f9f9;
                    border-left: 5px solid {depth_color};
                    padding: 20px;
                    border-radius: 6px;
                    margin-bottom: 20px;
                }}
                .depth-box h2 {{
                    margin-top: 0;
                    color: {depth_color};
                    font-size: 1.4em;
                }}
                .depth-box p {{
                    font-weight: bold;
                    color: {depth_color};
                    margin: 10px 0 0;
                }}
                .attachment {{
                    font-size: 1em;
                    margin-top: 20px;
                }}
                .footer {{
                    text-align: center;
                    padding: 20px;
                    font-size: 0.9em;
                    color: #6c757d;
                    border-top: 1px solid #dee2e6;
                    background-color: #f8f9fa;
                }}
                .footer p {{
                    margin: 5px 0;
                }}
            </style>
        </head>
        <body>

                <div class="content">
                    <div class="summary-box">
                        <h2>Meeting Summary</h2>
                        <p>The daily operational excellence meeting has been successfully completed.</p>
                    </div>

                    <div class="depth-box">
                        <h2>Meeting Depth</h2>
                        <p>{depth_label} — Duration: {int(duration_minutes)} minutes</p>
                    </div>

                    <p class="attachment">
                        <strong>Attached:</strong> Daily OpEx Report (PDF)
                    </p>
                </div>

                <div class="footer">
                    <p><strong>Daily OpEx System</strong></p>
                    <p>This is an automated notification.</p>
                    <p>Please do not reply to this email.</p>
                </div>
            </div>
        </body>
        </html>
        """


        # Attach HTML content
        msg.attach(MIMEText(html_content, "html"))

        # Find and attach the most recent PDF from your network folder
        pdf_path = find_latest_daily_opex_pdf()

        # CRITICAL: Do not send email if PDF is not found
        if not pdf_path or not os.path.exists(pdf_path):
            logger.error("CRITICAL: No PDF found - email will NOT be sent")
            return False

        # Try to attach the PDF
        try:
            with open(pdf_path, 'rb') as pdf_file:
                pdf_attachment = MIMEBase('application', 'pdf')
                pdf_attachment.set_payload(pdf_file.read())
                encoders.encode_base64(pdf_attachment)

                pdf_filename = os.path.basename(pdf_path)
                pdf_attachment.add_header('Content-Disposition', f'attachment; filename={pdf_filename}')
                msg.attach(pdf_attachment)

            logger.info(f"PDF attached to email: {pdf_filename} from {pdf_path}")
        except Exception as attach_error:
            logger.error(f"CRITICAL: Error attaching PDF to email: {str(attach_error)}")
            logger.error("Email will NOT be sent without PDF attachment")
            return False

        # Only send email if PDF was successfully attached
        all_recipients = recipients + cc_recipients
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(sender_email, all_recipients, msg.as_string())

        logger.info(f"Daily meeting completion email sent to: {', '.join(recipients)}")
        return True

    except Exception as e:
        logger.error(f"Error sending daily meeting completion email: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# ADD THIS HELPER FUNCTION - Finds your saved PDF
# ============================================================================

def find_latest_daily_opex_pdf():
    """
    Find the most recent Daily OpEx PDF from the network folder
    Uses the same path structure as your save_pdf function
    """
    try:
        # Base path where PDFs are saved (same as your save_pdf function)
        base_path = r'U:\Production\Daily OpEX Report'
        
        # Get current date for folder
        current_date = datetime.now()
        month_year = current_date.strftime('%B %Y')  # e.g., "November 2025"
        
        # Full path to today's folder
        folder_path = os.path.join(base_path, month_year)
        
        if not os.path.exists(folder_path):
            logger.warning(f"Folder does not exist: {folder_path}")
            return None
        
        # Get today's date string for filename matching
        today_str = current_date.strftime('%Y-%m-%d')  # e.g., "2025-11-04"
        
        # Look for PDF files from today
        import glob
        pdf_pattern = os.path.join(folder_path, f"*{today_str}*.pdf")
        pdf_files = glob.glob(pdf_pattern)
        
        if not pdf_files:
            # Try alternative pattern - maybe filename has different format
            pdf_pattern = os.path.join(folder_path, "*.pdf")
            pdf_files = glob.glob(pdf_pattern)
            
            # Filter to only today's files by modification time
            if pdf_files:
                today_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
                pdf_files = [f for f in pdf_files if datetime.fromtimestamp(os.path.getmtime(f)) >= today_start]
        
        if not pdf_files:
            logger.warning(f"No PDF files found in {folder_path} for today")
            return None
        
        # Get the most recent file
        latest_pdf = max(pdf_files, key=os.path.getmtime)
        logger.info(f"Found latest PDF: {latest_pdf}")
        return latest_pdf
        
    except Exception as e:
        logger.error(f"Error finding latest PDF: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


@app.route('/api/meeting-actions', methods=['GET', 'POST'])
def manage_meeting_actions():
    """API endpoint for meeting actions capture"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    if request.method == 'POST':
        try:
            action_data = request.get_json()
            
            # Generate session ID if not exists
            session_id = session.get('meeting_session_id')
            if not session_id:
                session_id = f"meeting_{datetime.now().strftime('%Y%m%d_%H%M')}_{session.get('userID', 'unknown')}"
                session['meeting_session_id'] = session_id
            
            query = """
            INSERT INTO DailyActions (
                SessionID, SourceType, SourceData, Department, 
                Assignee, DueDate, Priority, Notes, CreatedBy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Convert date if provided
            due_date = None
            if action_data.get('dueDate'):
                try:
                    due_date = datetime.fromisoformat(action_data['dueDate'].replace('Z', '+00:00'))
                except:
                    due_date = None
            
            conn = get_db_connection_1()
            cursor = conn.cursor()
            cursor.execute(query, [
                session_id,
                action_data.get('sourceType'),
                json.dumps(action_data.get('sourceData')),
                action_data.get('department', ''),
                action_data.get('assignee', ''),
                due_date,
                action_data.get('priority', 'Medium'),
                action_data.get('notes', ''),
                session.get('name', 'System')
            ])
            conn.commit()
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True, 
                'sessionId': session_id,
                'message': 'Action item captured successfully'
            })
            
        except Exception as e:
            logger.error(f"Error adding meeting action: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    else:  # GET
        try:
            session_id = request.args.get('session_id') or session.get('meeting_session_id')
            if not session_id:
                return jsonify([])
            
            query = """
            SELECT ActionID, SessionID, SourceType, SourceData, Department, 
                   Assignee, DueDate, Priority, Notes, Status, CreatedDate, 
                   CreatedBy, EmailSent, CompletedDate, CompletedBy
            FROM DailyActions 
            WHERE SessionID = ?
            ORDER BY CreatedDate DESC
            """
            
            conn = get_db_connection_1()
            df = pd.read_sql(query, conn, params=[session_id])
            conn.close()
            
            actions = []
            for _, row in df.iterrows():
                try:
                    source_data = json.loads(row['SourceData']) if row['SourceData'] else {}
                except:
                    source_data = {}
                
                actions.append({
                    'actionID': int(row['ActionID']),
                    'sessionID': row['SessionID'],
                    'sourceType': row['SourceType'],
                    'sourceData': source_data,
                    'department': row['Department'] or '',
                    'assignee': row['Assignee'] or '',
                    'dueDate': row['DueDate'].isoformat() if pd.notna(row['DueDate']) else '',
                    'priority': row['Priority'] or 'Medium',
                    'notes': row['Notes'] or '',
                    'status': row['Status'] or 'Pending',
                    'createdDate': row['CreatedDate'].isoformat() if pd.notna(row['CreatedDate']) else '',
                    'createdBy': row['CreatedBy'] or '',
                    'emailSent': bool(row['EmailSent']) if pd.notna(row['EmailSent']) else False
                })
            
            return jsonify(actions)
            
        except Exception as e:
            logger.error(f"Error getting meeting actions: {str(e)}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/meeting-actions/<int:action_id>', methods=['PUT', 'DELETE'])
def manage_single_meeting_action(action_id):
    """Update or delete a meeting action"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    if request.method == 'PUT':
        try:
            action_data = request.get_json()
            
            # Convert date if provided
            due_date = None
            if action_data.get('dueDate'):
                try:
                    due_date = datetime.fromisoformat(action_data['dueDate'].replace('Z', '+00:00'))
                except:
                    due_date = None
            
            query = """
            UPDATE DailyActions SET 
                Department = ?, Assignee = ?, DueDate = ?, Priority = ?, 
                Notes = ?, Status = ?
            WHERE ActionID = ?
            """
            
            conn = get_db_connection_1()
            cursor = conn.cursor()
            cursor.execute(query, [
                action_data.get('department', ''),
                action_data.get('assignee', ''),
                due_date,
                action_data.get('priority', 'Medium'),
                action_data.get('notes', ''),
                action_data.get('status', 'Pending'),
                action_id
            ])
            
            rows_affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            if rows_affected > 0:
                return jsonify({'success': True, 'message': 'Action updated successfully'})
            else:
                return jsonify({'error': 'Action not found'}), 404
                
        except Exception as e:
            logger.error(f"Error updating meeting action: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            query = "DELETE FROM DailyActions WHERE ActionID = ?"
            
            conn = get_db_connection_1()
            cursor = conn.cursor()
            cursor.execute(query, [action_id])
            
            rows_affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            if rows_affected > 0:
                return jsonify({'success': True, 'message': 'Action deleted successfully'})
            else:
                return jsonify({'error': 'Action not found'}), 404
                
        except Exception as e:
            logger.error(f"Error deleting meeting action: {str(e)}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/meeting-actions/sessions')
def get_meeting_sessions():
    """Get all meeting sessions for history"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        query = """
        SELECT SessionID, COUNT(*) as ActionCount, MIN(CreatedDate) as SessionDate,
               SUM(CASE WHEN Status = 'Pending' THEN 1 ELSE 0 END) as PendingCount
        FROM DailyActions 
        GROUP BY SessionID 
        ORDER BY MIN(CreatedDate) DESC
        """
        
        conn = get_db_connection_1()
        df = pd.read_sql(query, conn)
        conn.close()
        
        sessions = []
        for _, row in df.iterrows():
            sessions.append({
                'sessionID': row['SessionID'],
                'sessionDate': row['SessionDate'].isoformat() if pd.notna(row['SessionDate']) else '',
                'actionCount': int(row['ActionCount']),
                'pendingCount': int(row['PendingCount'])
            })
        
        return jsonify(sessions)
        
    except Exception as e:
        logger.error(f"Error getting meeting sessions: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/meeting-actions/new-session', methods=['POST'])
def start_new_meeting_session():
    """Start a new meeting session"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    session_id = f"meeting_{datetime.now().strftime('%Y%m%d_%H%M')}_{session.get('userID', 'unknown')}"
    session['meeting_session_id'] = session_id
    
    return jsonify({
        'success': True,
        'sessionId': session_id,
        'message': 'New meeting session started'
    })

# Photo upload routes
@app.route('/api/photos/upload/<incident_id>', methods=['POST'])
def upload_photo(incident_id):
    """Upload a photo for an incident"""
    try:
        if 'photo' not in request.files:
            return jsonify({'error': 'No photo file provided'}), 400
        
        file = request.files['photo']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if file and allowed_file(file.filename):
            # Create a simple filename: incident_id + timestamp + extension
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_extension = file.filename.rsplit('.', 1)[1].lower()
            filename = f"incident_{incident_id}_{timestamp}.{file_extension}"
            filename = secure_filename(filename)
            
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            logger.info(f"Photo uploaded for incident {incident_id}: {filename}")
            return jsonify({
                'success': True,
                'filename': filename,
                'message': 'Photo uploaded successfully'
            })
        else:
            return jsonify({'error': 'Invalid file type. Please use PNG, JPG, JPEG, or GIF'}), 400
            
    except Exception as e:
        logger.error(f"Error uploading photo: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/photos/<incident_id>')
def get_incident_photos(incident_id):
    """Get all photos for an incident"""
    try:
        photos = []
        if os.path.exists(app.config['UPLOAD_FOLDER']):
            for filename in os.listdir(app.config['UPLOAD_FOLDER']):
                if filename.startswith(f'incident_{incident_id}_'):
                    photos.append({
                        'filename': filename,
                        'url': f'/photos/{filename}'
                    })
        
        return jsonify(photos)
    except Exception as e:
        logger.error(f"Error getting photos: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/photos/<path:filename>')
def serve_photo_simple(filename):
    """Serve uploaded photos - simplified version"""
    try:
        # Basic validation - allow incident or maintenance photos
        if not (filename.startswith('incident_') or filename.startswith('maintenance_')):
            return jsonify({'error': 'Invalid file'}), 404
            
        # Check file extension
        allowed_extensions = {'.png', '.jpg', '.jpeg', '.gif'}
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in allowed_extensions:
            return jsonify({'error': 'Invalid file type'}), 404
        
        logger.info(f"Serving photo: {filename}")
        
        # Flask's send_from_directory handles security automatically
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)
        
    except Exception as e:
        logger.error(f"Error serving photo {filename}: {str(e)}")
        return jsonify({'error': 'Photo not found'}), 404

@app.route('/api/photos/cleanup')
def cleanup_old_photos():
    """Clean up photos older than 30 days (optional utility)"""
    try:
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            return jsonify({'message': 'No photos to clean up'})
        
        cutoff_date = datetime.now() - timedelta(days=30)
        deleted_count = 0
        
        for filename in os.listdir(app.config['UPLOAD_FOLDER']):
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            if os.path.isfile(filepath):
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_time < cutoff_date:
                    os.remove(filepath)
                    deleted_count += 1
        
        return jsonify({
            'success': True,
            'message': f'Cleaned up {deleted_count} old photos'
        })
    except Exception as e:
        logger.error(f"Error cleaning up photos: {str(e)}")
        return jsonify({'error': str(e)}), 500



# Database connections
def get_db_connection():
    """Get database connection with error handling"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=AT-SPBVMSQL01;'
            'DATABASE=G0X_Production;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
            'Encrypt=yes;'
            'Connection Timeout=30;'
        )
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def get_db_connection_1():
    """Get database connection with error handling"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=AT-SPBVMSQL01;'
            'DATABASE=G0X_Production_Test;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
            'Encrypt=yes;'
            'Connection Timeout=30;'
        )
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def get_weld_db_connection():
    """Get WELD_Production database connection"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=AT-SPBVMSQL01;'
            'DATABASE=WELD_Production;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
            'Encrypt=yes;'
            'Connection Timeout=30;'
        )
        logger.info("WELD_Production database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"WELD_Production database connection failed: {str(e)}")
        raise



# Safety Database Class
class SafetyDatabase:
    def __init__(self):
        try:
            conn = self.get_connection()
            conn.close()
            logger.info("SafetyDatabase initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize SafetyDatabase: {str(e)}")
            raise
    
    def get_connection(self):
        return get_db_connection()
    
    def execute_query(self, query, params=None):
        """Execute query with proper connection handling"""
        conn = None
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_weld_query(self, query, params=None):
        """Execute query against WELD_Production database"""
        conn = None
        try:
            conn = get_weld_db_connection()  # Uses your existing function
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"WELD query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_non_query(self, query, params=None):
        """Execute non-query (INSERT, UPDATE, DELETE) with proper connection handling"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Non-query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def get_all_safety_data(self):
        """Get all safety incidents from BOTH G0X and WELD databases combined"""
        try:
            # Get G0X_Production SHE data
            query_g0x = """
            SELECT s.*, m.dateEP 
            FROM dbo.SHE s
            LEFT JOIN dbo.Main m ON s.mainID = m.mainID
            ORDER BY s.MainID DESC
            """
            df_g0x = self.execute_query(query_g0x)
            df_g0x['source'] = 'G0X'
            
            # Get WELD_Production SHE data
            query_weld = """
            SELECT 
                [sheID],
                [mainID],
                [timeEvent] as time,
                [personalNumber],
                [reported],
                [operationID] as machineID,
                [description],
                [incidentType],
                [inmediateAction]
            FROM [WELD_Production].[dbo].[SHE]
            ORDER BY mainID DESC
            """
            
            try:
                df_weld = self.execute_weld_query(query_weld)
                df_weld['source'] = 'WELD'
                
                # Combine both databases into one DataFrame
                df_combined = pd.concat([df_g0x, df_weld], ignore_index=True, sort=False)
                
                # Sort by time (most recent first)
                if 'time' in df_combined.columns:
                    df_combined = df_combined.sort_values('time', ascending=False, na_position='last')
                
                logger.info(f"Combined {len(df_g0x)} G0X incidents + {len(df_weld)} WELD incidents = {len(df_combined)} total")
                
                return df_combined
                
            except Exception as weld_error:
                logger.warning(f"Could not fetch WELD data: {str(weld_error)}")
                # Return G0X data only if WELD fails
                return df_g0x
                
        except Exception as e:
            logger.error(f"Error in get_all_safety_data: {str(e)}")
            raise
    
    def get_recent_incidents(self, days=None):
        """Get recent incidents from BOTH G0X and WELD databases combined"""
        if days is None:
            today = datetime.now().weekday()
            if today == 0:
                days = 3
            else:
                days = 1
        
        try:
            # Calculate hours with buffer: 72 hours for Monday (3 days), 28 hours for 1 day
            # This ensures we catch incidents from early previous day
            if days == 1:
                hours = 27  # Same as get_today_incidents for consistency
            else:
                hours = days * 27
            
            # Get G0X incidents from last N hours
            query_g0x = """
            SELECT s.*, m.dateEP 
            FROM dbo.SHE s
            LEFT JOIN dbo.Main m ON s.mainID = m.mainID
            WHERE s.time >= DATEADD(hour, ?, GETDATE())
            ORDER BY s.time DESC
            """
            df_g0x = self.execute_query(query_g0x, params=[-hours])
            df_g0x['source'] = 'G0X'
            
            # Get WELD incidents from last N days
            query_weld = """
            SELECT 
                [sheID],
                [mainID],
                [timeEvent] as time,
                [personalNumber],
                [reported],
                [operationID] as machineID,
                [description],
                [incidentType],
                [inmediateAction]
            FROM [WELD_Production].[dbo].[SHE]
            WHERE timeEvent >= DATEADD(hour, ?, GETDATE())
            ORDER BY timeEvent DESC
            """
            
            try:
                df_weld = self.execute_weld_query(query_weld, params=[-hours])
                df_weld['source'] = 'WELD'
                
                # Combine into single DataFrame
                df_combined = pd.concat([df_g0x, df_weld], ignore_index=True, sort=False)
                
                # Sort by time (most recent first)
                if 'time' in df_combined.columns:
                    df_combined = df_combined.sort_values('time', ascending=False, na_position='last')
                
                logger.info(f"Recent incidents: {len(df_g0x)} G0X + {len(df_weld)} WELD = {len(df_combined)} total")
                
                return df_combined
                
            except Exception as weld_error:
                logger.warning(f"Could not fetch WELD recent incidents: {str(weld_error)}")
                return df_g0x
                
        except Exception as e:
            logger.error(f"Error in get_recent_incidents: {str(e)}")
            raise
    
    def get_rotation_exceptions(self):
        """Get rotation sheet exception counts from last 3 rotation sheets per line (Front/Rear)"""
        try:
            conn = None
            try:
                conn = get_db_connection()  # Production database
                
                # Get the last 3 rotation sheet IDs for Front lines (FCE, FCO)
                front_query = """
                WITH LastThreeSheets AS (
                    SELECT DISTINCT TOP 3 ID_RotationSheet
                    FROM [G0X_Production].[dbo].[Matrix_RotationSheet_Changes_LOG]
                    WHERE Line IN ('FCE', 'FCO')
                    ORDER BY ID_RotationSheet DESC
                )
                SELECT 
                    rs.ID_RotationSheet,
                    COUNT(*) as exception_count
                FROM [G0X_Production].[dbo].[Matrix_RotationSheet_Changes_LOG] rs
                WHERE rs.Line IN ('FCE', 'FCO')
                AND rs.ID_RotationSheet IN (SELECT ID_RotationSheet FROM LastThreeSheets)
                GROUP BY rs.ID_RotationSheet
                ORDER BY rs.ID_RotationSheet DESC
                """
                
                # Get the last 3 rotation sheet IDs for Rear lines (RCE, RCO)
                rear_query = """
                WITH LastThreeSheets AS (
                    SELECT DISTINCT TOP 3 ID_RotationSheet
                    FROM [G0X_Production].[dbo].[Matrix_RotationSheet_Changes_LOG]
                    WHERE Line IN ('RCE', 'RCO')
                    ORDER BY ID_RotationSheet DESC
                )
                SELECT 
                    rs.ID_RotationSheet,
                    COUNT(*) as exception_count
                FROM [G0X_Production].[dbo].[Matrix_RotationSheet_Changes_LOG] rs
                WHERE rs.Line IN ('RCE', 'RCO')
                AND rs.ID_RotationSheet IN (SELECT ID_RotationSheet FROM LastThreeSheets)
                GROUP BY rs.ID_RotationSheet
                ORDER BY rs.ID_RotationSheet DESC
                """
                
                # Execute queries
                front_df = pd.read_sql(front_query, conn)
                rear_df = pd.read_sql(rear_query, conn)
                
                # Process front line data
                front_sheets = []
                if not front_df.empty:
                    for _, row in front_df.iterrows():
                        front_sheets.append({
                            'id': int(row['ID_RotationSheet']),
                            'count': int(row['exception_count'])
                        })
                
                # Process rear line data
                rear_sheets = []
                if not rear_df.empty:
                    for _, row in rear_df.iterrows():
                        rear_sheets.append({
                            'id': int(row['ID_RotationSheet']),
                            'count': int(row['exception_count'])
                        })
                
                result = {
                    'front_sheets': front_sheets,
                    'rear_sheets': rear_sheets,
                    'front_total': sum(sheet['count'] for sheet in front_sheets),
                    'rear_total': sum(sheet['count'] for sheet in rear_sheets),
                    'total_exceptions': sum(sheet['count'] for sheet in front_sheets) + sum(sheet['count'] for sheet in rear_sheets)
                }
                
                logger.info(f"Rotation exceptions retrieved: {result}")
                return result
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting rotation exceptions: {str(e)}")
            return {
                'front_sheets': [],
                'rear_sheets': [],
                'front_total': 0,
                'rear_total': 0,
                'total_exceptions': 0
            }
    
    def get_today_incidents(self):
        """Get today's incidents from BOTH G0X and WELD databases combined"""
        today = datetime.now().weekday()
        hours = 72 if today == 0 else 27
        
        try:
            # Get G0X incidents from last N hours
            query_g0x = """
            SELECT s.*, m.dateEP 
            FROM dbo.SHE s
            LEFT JOIN dbo.Main m ON s.mainID = m.mainID
            WHERE s.time >= DATEADD(hour, ?, GETDATE())
            ORDER BY s.time DESC
            """
            df_g0x = self.execute_query(query_g0x, params=[-hours])
            df_g0x['source'] = 'G0X'
            
            # Get WELD incidents from last N hours
            query_weld = """
            SELECT 
                [sheID],
                [mainID],
                [timeEvent] as time,
                [personalNumber],
                [reported],
                [operationID] as machineID,
                [description],
                [incidentType],
                [inmediateAction]
            FROM [WELD_Production].[dbo].[SHE]
            WHERE timeEvent >= DATEADD(hour, ?, GETDATE())
            ORDER BY timeEvent DESC
            """
            
            try:
                df_weld = self.execute_weld_query(query_weld, params=[-hours])
                df_weld['source'] = 'WELD'
                
                # Combine into single DataFrame
                df_combined = pd.concat([df_g0x, df_weld], ignore_index=True, sort=False)
                
                # Sort by time (most recent first)
                if 'time' in df_combined.columns:
                    df_combined = df_combined.sort_values('time', ascending=False, na_position='last')
                
                logger.info(f"Today's incidents: {len(df_g0x)} G0X + {len(df_weld)} WELD = {len(df_combined)} total")
                
                return df_combined
                
            except Exception as weld_error:
                logger.warning(f"Could not fetch WELD today incidents: {str(weld_error)}")
                return df_g0x
                
        except Exception as e:
            logger.error(f"Error in get_today_incidents: {str(e)}")
            raise

    
    def get_safety_summary(self):
        """Get safety summary statistics with daily focus using dateEP from Main table"""
        try:
            # Get all incidents with dateEP from Main table
            df = self.get_all_safety_data()  # This now includes dateEP
            
            if df.empty:
                logger.warning("No safety data found")
                return {
                    'total_incidents': 0,
                    'first_aid_count': 0,
                    'near_miss_count': 0,
                    'recordable_count': 0,
                    'today_count': 0,
                    'week_count': 0,
                    'month_count': 0,
                    'days_since_last': 0
                }

            # Calculate metrics
            total_incidents = len(df)
            first_aid_count = len(df[df['incidentType'] == 'First-Aid']) if 'incidentType' in df.columns else 0
            near_miss_count = len(df[df['incidentType'] == 'Near-Miss']) if 'incidentType' in df.columns else 0
            recordable_count = len(df[df['incidentType'] == 'Recordable']) if 'incidentType' in df.columns else 0

            # Apply Monday exception for "today's" incidents
            today = datetime.now().weekday()  # Monday = 0, Sunday = 6
            if today == 0:  # Monday
                # Get last 3 days
                today_df = self.get_recent_incidents(3)
                period_label = "last 3 days"
            else:
                # Get today only
                today_df = self.get_today_incidents()
                period_label = "today"
            
            today_count = len(today_df)
            
            # Week incidents (always 7 days)
            week_df = self.get_recent_incidents(7)
            week_count = len(week_df)
            
            # Month incidents (always 30 days)
            month_df = self.get_recent_incidents(30)
            month_count = len(month_df)
            
            # Calculate days since last incident - prioritize time field for accuracy
            days_since_last = 0
            
            # First check if there are incidents today
            if not today_df.empty:
                days_since_last = 0
            elif not df.empty and 'time' in df.columns:
                # Use time field (actual incident time) for accuracy
                df['time'] = pd.to_datetime(df['time'], errors='coerce')
                valid_time_dates = df[df['time'].notna()]
                if not valid_time_dates.empty:
                    last_incident_date = valid_time_dates['time'].max()
                    days_since_last = max(0, (datetime.now() - last_incident_date).days)

            summary = {
                'total_incidents': total_incidents,
                'first_aid_count': first_aid_count,
                'near_miss_count': near_miss_count,
                'recordable_count': recordable_count,
                'today_count': today_count,
                'week_count': week_count,
                'month_count': month_count,
                'recent_count': today_count,  # For backward compatibility
                'days_since_last': days_since_last,
                'period_label': period_label  # Add this to know what period we're showing
            }
            
            logger.info(f"Safety summary generated: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating safety summary: {str(e)}")
            raise
    
    def get_incident_by_type(self, days=None):
        """Get incident counts by type for a specific period"""
        try:
            if days:
                df = self.get_recent_incidents(days)
            else:
                df = self.get_all_safety_data()
                
            if df.empty or 'incidentType' not in df.columns:
                logger.warning("No incident type data found")
                return {}
            
            type_counts = df['incidentType'].value_counts()
            result = type_counts.to_dict()
            logger.info(f"Incident types: {result}")
            return result
        except Exception as e:
            logger.error(f"Error getting incidents by type: {str(e)}")
            raise
    
    def get_daily_trend(self, days=30):
        """Get daily incident trend using dateEP from Main table"""
        try:
            df = self.get_recent_incidents(days)
            if df.empty or 'dateEP' not in df.columns:
                logger.warning("No trend data found")
                return {}
            
            df['dateEP'] = pd.to_datetime(df['dateEP'])
            df['date'] = df['dateEP'].dt.date
            daily_counts = df['date'].value_counts().sort_index()
            
            # Convert to format suitable for charts
            result = {}
            for date_obj, count in daily_counts.items():
                result[str(date_obj)] = count
            
            logger.info(f"Daily trend data points: {len(result)}")
            return result
        except Exception as e:
            logger.error(f"Error getting daily trend: {str(e)}")
            raise

    def get_active_associates(self):
        """Get active associates for personal number selection"""
        try:
            query = """
            SELECT personalNumber, name 
            FROM Associate 
            WHERE status = 1
            ORDER BY name
            """
            df = self.execute_query(query)
            
            associates = []
            for _, row in df.iterrows():
                associates.append({
                    'personalNumber': str(row.get('personalNumber', '')),
                    'name': str(row.get('name', ''))
                })
            
            return associates
            
        except Exception as e:
            logger.error(f"Error getting active associates: {str(e)}")
            raise
    
    def get_active_machines(self):
        """Get active machines for machine selection"""
        try:
            query = """
            SELECT machineID, machine 
            FROM Machine 
            ORDER BY machine
            """
            df = self.execute_query(query)
            
            machines = []
            for _, row in df.iterrows():
                machines.append({
                    'machineID': str(row.get('machineID', '')),
                    'machine': str(row.get('machine', ''))
                })
            
            return machines
            
        except Exception as e:
            logger.error(f"Error getting active machines: {str(e)}")
            # If there's an error, try without any WHERE clause
            try:
                query_alt = """
                SELECT machineID, machine 
                FROM Machine 
                ORDER BY machine
                """
                df = self.execute_query(query_alt)
                
                machines = []
                for _, row in df.iterrows():
                    machines.append({
                        'machineID': str(row.get('machineID', '')),
                        'machine': str(row.get('machine', ''))
                    })
                
                return machines
            except Exception as e2:
                logger.error(f"Error getting machines (alternative query): {str(e2)}")
                raise

    def get_recent_main_records(self, limit=2):
        """Get recent main records for incident association - with supervisor names"""
        try:
            query = """
            SELECT TOP (?) 
                m.mainID, 
                m.dateAccess,
                COALESCE(u.name, 'Unknown') as supervisor_name
            FROM dbo.Main m
            LEFT JOIN dbo.Users u ON m.userID = u.userID
            ORDER BY m.dateAccess DESC
            """
            df = self.execute_query(query, params=[limit])
            
            main_records = []
            for _, row in df.iterrows():
                main_records.append({
                    'mainID': str(row.get('mainID', '')),
                    'dateAccess': str(row.get('dateAccess', '')),
                    'supervisor_name': str(row.get('supervisor_name', 'Unknown'))
                })
            
            return main_records
            
        except Exception as e:
            logger.error(f"Error getting main records: {str(e)}")
            raise
    
    def add_incident(self, incident_data):
        """Add a new safety incident with correct column mapping"""
        try:
            # Helper function to truncate strings to safe lengths based on actual DB column sizes
            def safe_string(value, max_length=255):
                if value is None:
                    return ''
                str_value = str(value)
                if len(str_value) > max_length:
                    logger.warning(f"Truncating field from {len(str_value)} to {max_length} characters")
                    return str_value[:max_length]
                return str_value
            
            # Include both xWhat and xWhere as they should be in the database
            query = """
            INSERT INTO dbo.SHE (
                mainID, time, incidentType, machineID, description, 
                inmediateAction, personalNumber, reported, xWhat, xWhere
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Get the immediate action with correct length limit
            immediate_action = incident_data.get('inmediateAction', '')
            if not immediate_action:
                immediate_action = 'No immediate action specified'
            immediate_action = safe_string(immediate_action, 200)  # varchar(200)
            
            # Get description with correct length limit
            description = incident_data.get('description', '')
            description = safe_string(description, 450)  # varchar(450)
            
            # Get location for xWhere with correct length limit
            location = incident_data.get('location', '') or incident_data.get('machineID', '') or 'Not specified'
            location = safe_string(location, 15)  # varchar(15)
            
            # Get other fields with safe string conversion and correct lengths
            machine_id = safe_string(incident_data.get('machineID', ''), 50)  # smallint but as string
            incident_type = safe_string(incident_data.get('incidentType', ''), 25)  # varchar(25)
            personal_number = safe_string(incident_data.get('personalNumber', ''), 20)  # int but as string
            
            # For xWhat, also limit to 15 characters like xWhere
            x_what = safe_string(description, 15)  # varchar(15)
            
            params = [
                incident_data.get('mainID'),           # mainID - Required from user selection
                incident_data.get('time'),             # time - When it happened
                incident_type,                         # incidentType - Type of incident
                machine_id,                            # machineID - Machine/area ID
                description,                           # description - What happened
                immediate_action,                      # inmediateAction - Immediate action taken
                personal_number,                       # personalNumber - Who reported it
                'Yes',                                 # reported - Mark as reported (varchar(3))
                x_what,                                # xWhat - What happened (varchar(15))
                location                               # xWhere - Where it happened (varchar(15))
            ]
            
            logger.info(f"Adding incident with field lengths - desc: {len(description)}, action: {len(immediate_action)}, location: {len(location)}, xWhat: {len(x_what)}")
            
            rows_affected = self.execute_non_query(query, params)
            logger.info(f"Added incident, rows affected: {rows_affected}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding incident: {str(e)}")
            raise
    
    def update_incident(self, incident_id, incident_data):
        """Update an existing safety incident with correct column mapping"""
        try:
            # Include both xWhat and xWhere as they should be in the database
            query = """
            UPDATE dbo.SHE SET 
                time = ?, incidentType = ?, machineID = ?, description = ?,
                inmediateAction = ?, personalNumber = ?, xWhat = ?, xWhere = ?
            WHERE sheID = ?
            """
            
            # Helper function to truncate strings to safe lengths based on actual DB column sizes
            def safe_string(value, max_length=255):
                if value is None:
                    return ''
                str_value = str(value)
                if len(str_value) > max_length:
                    logger.warning(f"Truncating field from {len(str_value)} to {max_length} characters")
                    return str_value[:max_length]
                return str_value
            
            # Get the immediate action with proper fallback and correct length limit
            immediate_action = incident_data.get('inmediateAction', '')
            if not immediate_action or immediate_action == 'None':
                immediate_action = 'No immediate action specified'
            immediate_action = safe_string(immediate_action, 200)  # varchar(200)
            
            # Get description with correct length limit
            description = incident_data.get('description', '')
            if not description:
                description = 'No description provided'
            description = safe_string(description, 450)  # varchar(450)
            
            # Get location for xWhere with correct length limit
            location = incident_data.get('location', '') or incident_data.get('machineID', '') or 'Not specified'
            location = safe_string(location, 15)  # varchar(15)
            
            # Get other fields with safe string conversion and correct lengths
            machine_id = safe_string(incident_data.get('machineID', ''), 50)  # smallint but as string
            incident_type = safe_string(incident_data.get('incidentType', ''), 25)  # varchar(25)
            personal_number = safe_string(incident_data.get('personalNumber', ''), 20)  # int but as string
            
            # For xWhat, also limit to 15 characters like xWhere
            x_what = safe_string(description, 15)  # varchar(15)
            
            # Ensure all string fields are not None and within limits
            params = [
                incident_data.get('time'),
                incident_type,
                machine_id,
                description,                          # description (varchar(450))
                immediate_action,                     # inmediateAction (varchar(200))
                personal_number,
                x_what,                              # xWhat - what happened (varchar(15))
                location,                            # xWhere - where it happened (varchar(15))
                incident_id
            ]
            
            # Log the parameters for debugging
            logger.info(f"Updating incident {incident_id}")
            logger.info(f"Field lengths - desc: {len(description)}, action: {len(immediate_action)}, location: {len(location)}, xWhat: {len(x_what)}")
            
            rows_affected = self.execute_non_query(query, params)
            logger.info(f"Updated incident {incident_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error updating incident {incident_id}: {str(e)}")
            logger.error(f"Incident data: {incident_data}")
            raise
    
    def delete_incident(self, incident_id):
        """Delete a safety incident"""
        try:
            query = "DELETE FROM dbo.SHE WHERE sheID = ?"
            rows_affected = self.execute_non_query(query, [incident_id])
            logger.info(f"Deleted incident {incident_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error deleting incident: {str(e)}")
            raise
    
    def get_incident_by_id(self, incident_id):
        """Get a specific incident by ID"""
        try:
            query = "SELECT * FROM dbo.SHE WHERE sheID = ?"
            df = self.execute_query(query, params=[incident_id])
            
            if df.empty:
                return None
            
            # Convert to dictionary and handle datetime serialization
            incident = df.iloc[0].to_dict()
            
            # Convert datetime objects to strings for JSON serialization
            for key, value in incident.items():
                if pd.isna(value):
                    incident[key] = None
                elif hasattr(value, 'isoformat'):  # datetime object
                    incident[key] = value.isoformat()
                elif isinstance(value, (pd.Timestamp, datetime)):
                    incident[key] = str(value)
                else:
                    # Ensure all values are strings to avoid data type issues
                    incident[key] = str(value) if value is not None else ''
            
            # Map database columns to expected field names
            mapped_incident = {
                'id': incident.get('sheID'),
                'time': incident.get('time'),
                'incidentType': incident.get('incidentType', ''),
                'machineID': incident.get('machineID', ''),
                'description': incident.get('description', ''),
                'inmediateAction': incident.get('inmediateAction', ''),  # Note: correct spelling from DB
                'personalNumber': incident.get('personalNumber', ''),
                'reportedBy': incident.get('reportedBy', ''),
                'severity': 'Medium',  # Default value since not in DB
                'status': 'Open',      # Default value since not in DB
                'location': incident.get('xWhere', ''),
                'mainID': incident.get('mainID', '')  # Include mainID for editing
            }
            
            # Ensure no None values that could cause SQL issues
            for key, value in mapped_incident.items():
                if value is None:
                    mapped_incident[key] = ''
            
            return mapped_incident
            
        except Exception as e:
            logger.error(f"Error getting incident by ID: {str(e)}")
            raise

@app.route('/api/safety/rotation-exceptions')
def rotation_exceptions():
    """API endpoint for rotation sheet exceptions"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        exceptions = safety_db.get_rotation_exceptions()
        return jsonify(exceptions)
    except Exception as e:
        logger.error(f"API error in rotation_exceptions: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Quality Database Class - CORRECTED VERSION
class QualityDatabase:
    def __init__(self):
        try:
            conn = self.get_connection()
            conn.close()
            logger.info("QualityDatabase initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize QualityDatabase: {str(e)}")
            raise



    @app.route('/api/quality/sdr/events')
    def quality_sdr_events():
        """API endpoint for SDR events (for quality dashboard)"""
        try:
            today = datetime.now().weekday()
            if today == 0:  # Monday
                hours_back = -72
            else:
                hours_back = -24
            
            conn = None
            try:
                conn = get_db_connection()
                query = """
                SELECT 
                    Detail,
                    Quantity,
                    Line,
                    DateClosed,
                    OK,
                    NOK
                FROM dbo.SupplierIssue 
                WHERE DateClosed >= DATEADD(hour, ?, GETDATE())
                ORDER BY DateClosed DESC
                """
                df = pd.read_sql(query, conn, params=[hours_back])
                
                events = []
                for _, row in df.iterrows():
                    date_closed = row.get('DateClosed')
                    if pd.notna(date_closed):
                        date_closed_str = date_closed.isoformat() if hasattr(date_closed, 'isoformat') else str(date_closed)
                    else:
                        date_closed_str = 'N/A'
                    
                    # Get raw values from database
                    ok_raw = str(row.get('OK', '')) if pd.notna(row.get('OK')) else None
                    nok_raw = str(row.get('NOK', '')) if pd.notna(row.get('NOK')) else None
                    
                    # Clean and extract just the filename
                    ok_image = None
                    nok_image = None
                    
                    if ok_raw and ok_raw.lower() not in ['none', 'null', '']:
                        # FIXED: Handle both forward slashes and backslashes
                        # Split on both / and \ to extract just the filename
                        ok_filename = ok_raw.replace('\\', '/').split('/')[-1].strip()
                        # Remove any URL-encoded characters or special prefixes
                        ok_filename = ok_filename.replace('%0C', '').replace('C:', '').replace('fakepath', '')
                        ok_filename = ok_filename.strip('/')
                        if ok_filename and ok_filename.lower() not in ['none', 'null', '']:
                            ok_image = ok_filename
                    
                    if nok_raw and nok_raw.lower() not in ['none', 'null', '']:
                        # FIXED: Handle both forward slashes and backslashes
                        nok_filename = nok_raw.replace('\\', '/').split('/')[-1].strip()
                        # Remove any URL-encoded characters or special prefixes
                        nok_filename = nok_filename.replace('%0C', '').replace('C:', '').replace('fakepath', '')  # ← Added ''
                        nok_filename = nok_filename.strip('/')
                        if nok_filename and nok_filename.lower() not in ['none', 'null', '']:
                            nok_image = nok_filename
                    
                    events.append({
                        'detail': str(row.get('Detail', '') if pd.notna(row.get('Detail')) else ''),
                        'quantity': int(row.get('Quantity', 0)) if pd.notna(row.get('Quantity')) else 0,
                        'line': str(row.get('Line', '') if pd.notna(row.get('Line')) else ''),
                        'dateClosed': date_closed_str,
                        'okImage': ok_image,    # Just the filename
                        'nokImage': nok_image   # Just the filename
                    })
                
                return jsonify(events)
                    
            finally:
                if conn:
                    conn.close()
                        
        except Exception as e:
            logger.error(f"API error in quality_sdr_events: {str(e)}")
            return jsonify({'error': str(e)}), 500
        
    @app.route('/static7/<path:filename>')
    def static7(filename):
        return send_from_directory('Z:\\Dell_Front_2_Photos\\EOL_Front_Dell_Tablet_2\\', filename)
        # return send_from_directory('D:\\Production\\CameraRollFront\\', filename)
    #//********************************************************************************************************************************
    @app.route('/static2/<path:filename>')
    def static2(filename):
        return send_from_directory('Z:\\Dell_Rear_2_Photos\\EOL_Rear_Dell_Tablet_2\\', filename)
        # return send_from_directory('D:\\Production\\CameraRollRear\\', filename)
    #//********************************************************************************************************************************

    @app.route('/static4/<path:filename>')
    def static4(filename):
        return send_from_directory('U:\\Quality\\1. Non Conformances\\4. Quality Alerts\\Pictures\\', filename)
    #//********************************************************************************************************************************  
    @app.route('/static5/<path:filename>')
    def static5(filename):
        return send_from_directory('Z:\\Dell_Front_1_Photos\\EOL_Front_Dell_Tablet_1\\', filename)
    #//********************************************************************************************************************************
    @app.route('/static6/<path:filename>')
    def static6(filename):
        return send_from_directory('Z:\\Dell_Rear_1_Photos\\EOL_Rear_Dell_Tablet_1\\', filename)
    ################################################################################################################


    @app.route('/static1/<path:filename>')
    def static1(filename):
        """Serve images from Quality Alerts Pictures directory"""
        return send_from_directory('U:\\Quality\\1. Non Conformances\\4. Quality Alerts\\Pictures\\', filename)


    @app.route("/api/quality-alert/<int:alert_id>", methods=['GET'])
    def get_quality_alert_details(alert_id):
        if not session.get('logged_in'):
            return jsonify({'error': 'Unauthorized'}), 401
        
        try:
            # Get the quality alert data using your existing functions
            operations = RPT_Operation(alert_id)
            materials = RPT_Material(alert_id)
            main_data = RPT_Main(alert_id)
            
            if not (operations and materials and main_data):
                return jsonify({'error': 'Quality alert not found'}), 404
            
            main = main_data[0]  # Get the first row
            
            # Build image URLs using your existing static route functions
            image1_url = None
            image2_url = None
            
            if main[14]:  # If image1 exists
                try:
                    image1_url = url_for('static1', filename=main[14])
                except:
                    try:
                        image1_url = url_for('static2', filename=main[14])
                    except:
                        try:
                            image1_url = url_for('static3', filename=main[14])
                        except:
                            pass
            
            if main[15]:  # If image2 exists
                try:
                    image2_url = url_for('static1', filename=main[15])
                except:
                    try:
                        image2_url = url_for('static2', filename=main[15])
                    except:
                        try:
                            image2_url = url_for('static3', filename=main[15])
                        except:
                            pass
            
            # Format the data for the frontend
            response_data = {
                'qaNumber': alert_id,
                'problemDescription': f"On {main[5]}, {main[17]}, {main[18]} at the {main[3]}, {main[11]} part(s) with {main[7]} on the {materials[0][2]} {'were' if main[11] > 1 else 'was'} detected by {main[2]}.",
                'operation': main[4],
                'shift': main[13],
                'expirationDate': main[21],
                'cleanPoint': main[22],
                'qualityAuth': main[19],
                'mfgAuth': main[20],
                'reportedBy': main[12],
                'instructions': [{'machine': getattr(op, 'Machine', ''), 'instruction': getattr(op, 'Instruction', '')} for op in operations],
                'materials': [{'description': mat[2] if len(mat) > 2 else ''} for mat in materials],
                'image1': image1_url,
                'image2': image2_url,
            }
            
            return jsonify(response_data)
            
        except Exception as e:
            print(f"Error fetching quality alert {alert_id}: {str(e)}")
            return jsonify({'error': f'Failed to load quality alert: {str(e)}'}), 500
        


        
    
    def get_all_scrap_data(self, hours=72):
        """Get ALL scrap data from G0X_Production (not filtered by selection)"""
        conn = None
        try:
            conn = get_db_connection()  # Use production database directly
            query = """
            SELECT 
                qmt.qualityID,         
                qmt.comment,
                qmat.Description as materialDescription,  -- Get Description from Quality_Material
                qmt.QualityMainID,
                qmr.date,
                qmr.qualityMainID as mainReportID
            FROM dbo.QualityModuleTech qmt
            INNER JOIN dbo.Quality_MainReport qmr ON qmt.QualityMainID = qmr.qualityMainID
            LEFT JOIN dbo.Quality_Material qmat ON qmt.materialID = qmat.BentelerNumber  -- FIXED: Use qmt.materialID instead of qmr.materialID
            WHERE qmr.date >= DATEADD(hour, ?, GETDATE())
            ORDER BY qmr.date DESC
            """
            df = pd.read_sql(query, conn, params=[-hours])
            
            scrap_items = []
            for _, row in df.iterrows():
                quality_id = str(row.get('qualityID', ''))
                quality_main_id = str(row.get('QualityMainID', ''))
                date_recorded = row.get('date')
                if pd.notna(date_recorded) and hasattr(date_recorded, 'isoformat'):
                    recorded_date = date_recorded.isoformat()
                else:
                    recorded_date = str(date_recorded) if date_recorded else 'N/A'
                
                # Access the global selected_scrap_reports variable
                global selected_scrap_reports
                
                scrap_items.append({
                    'qualityID': quality_id,
                    'qualityMainID': quality_main_id,
                    'mainReportID': str(row.get('mainReportID', '')),
                    'date': recorded_date,
                    'comment': str(row.get('comment', '') if pd.notna(row.get('comment')) else ''),
                    'why': str(row.get('materialDescription', '') if pd.notna(row.get('materialDescription')) else ''),  # From Quality_Material.Description
                    'isSelected': quality_id in selected_scrap_reports
                })
            
            return scrap_items
            
        except Exception as e:
            logger.error(f"Error getting all scrap data from G0X_Production: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()


    
    def get_connection(self):
        return get_db_connection_1()
    
    def execute_query(self, query, params=None):
        """Execute query with proper connection handling"""
        conn = None
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Quality query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_non_query(self, query, params=None):
        """Execute non-query (INSERT, UPDATE, DELETE) with proper connection handling"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Quality non-query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def debug_table_columns(self, table_name):
        """Debug helper to see actual column names"""
        try:
            query = f"SELECT TOP 1 * FROM dbo.{table_name}"
            df = self.execute_query(query)
            logger.info(f"Columns in {table_name}: {list(df.columns)}")
            return list(df.columns)
        except Exception as e:
            logger.error(f"Error checking columns for {table_name}: {str(e)}")
            return []

    def get_quality_summary(self):
        """Get quality summary statistics"""
        try:
            summary = {}
            
        # Get BMW/VW Quality Claims (last 48 hours)
        # Get BMW/VW Quality Claims (last 48 hours) - FROM TEST DB
            claims_query = """
            SELECT COUNT(*) as count FROM dbo.DailyBMWQualityClaims 
            WHERE dateTime >= DATEADD(hour, -24, GETDATE())
            """
            claims_df = self.execute_query(claims_query)
            summary['quality_claims_24h'] = int(claims_df.iloc[0]['count']) if not claims_df.empty else 0
            
            # Get Key Turns (last 48 hours) - FROM TEST DB
            key_turns_query = """
            SELECT COUNT(*) as count FROM dbo.DailyKeyTurns 
            WHERE dateTime >= DATEADD(hour, -24, GETDATE())
            """
            key_turns_df = self.execute_query(key_turns_query)
            summary['key_turns_24h'] = int(key_turns_df.iloc[0]['count']) if not key_turns_df.empty else 0
            
            # Get Line Quality Issues (last 24 hours) - FROM TEST DB
            line_quality_query = """
            SELECT COUNT(*) as count FROM dbo.DailyLineQuality 
            WHERE dateTime >= DATEADD(hour, -24, GETDATE())
            """
            line_quality_df = self.execute_query(line_quality_query)
            summary['eol_issues_24h'] = int(line_quality_df.iloc[0]['count']) if not line_quality_df.empty else 0
            
            # NEW: Get EOL Findings (last 24 hours) - FROM PRODUCTION DB
            conn = None
            try:
                conn = get_db_connection()  # Use production database directly

                eol_findings_query = """
                    SELECT COUNT(*) AS count
                    FROM dbo.QualityBMWLOG bmw
                    INNER JOIN dbo.main main
                        ON main.qualityEOLID = bmw.qualityEOLID
                    WHERE bmw.dateRecorded >= DATEADD(hour, -24, GETDATE())
                    AND main.closed = 'Yes'
                    """

                eol_findings_df = pd.read_sql(eol_findings_query, conn)
                summary['eol_findings_24h'] = int(eol_findings_df.iloc[0]['count']) if not eol_findings_df.empty else 0
            finally:
                if conn:
                    conn.close()
            
            # NEW: Active Quality Alerts - FROM PRODUCTION DB
            conn = None
            try:
                conn = get_db_connection()  # Use production database directly
                quality_alerts_query = """
                SELECT COUNT(*) as count FROM dbo.QualityAlert_Main 
                WHERE FechaFinal > CAST(GETDATE() AS DATE)
                """
                quality_alerts_df = pd.read_sql(quality_alerts_query, conn)
                summary['quality_alerts_active'] = int(quality_alerts_df.iloc[0]['count']) if not quality_alerts_df.empty else 0
            finally:
                if conn:
                    conn.close()
            
            
            # Get active supplier containments - FROM TEST DB
            containment_query = """
            SELECT COUNT(*) as count FROM dbo.DailySupplierContainment 
            """
            containment_df = self.execute_query(containment_query)
            summary['active_containments'] = int(containment_df.iloc[0]['count']) if not containment_df.empty else 0
            
            # Calculate scrap cost (mock data)
            conn = None
            try:
                conn = get_db_connection()  # Use production database directly
                scrap_query = """
                SELECT COUNT(*) as count 
                FROM dbo.QualityModuleTech qmt 
                INNER JOIN dbo.Quality_MainReport qmr ON qmt.QualityMainID = qmr.qualityMainID
                WHERE qmr.date >= DATEADD(hour, -72, GETDATE())
                """
                scrap_df = pd.read_sql(scrap_query, conn)
                summary['scrap_reports_24h'] = int(scrap_df.iloc[0]['count']) if not scrap_df.empty else 0
            finally:
                if conn:
                    conn.close()


            # MRB parts (mock)
            mrb_query = """
            SELECT COUNT(*) as count FROM dbo.InventoryCage 
            WHERE Disposition = 'Hold' AND inCage = 'Yes'            
            """
            mrb_df = self.execute_query(mrb_query)
            summary['mrb_parts'] = int(mrb_df.iloc[0]['count']) if not mrb_df.empty else 0
            summary['sdr_count_24h'] = self.get_sdr_summary()

            
            logger.info(f"Quality summary generated (EOL Findings and Quality Alerts from Production, others from Test): {summary}")
            return summary
        

            
        except Exception as e:
            logger.error(f"Error generating quality summary: {str(e)}")
            raise

    

    def get_all_mrb_data(self):
        """Get ALL MRB data (not filtered by selection)"""
        try:
            query = """
            SELECT 
                PartDescription, 
                Defect, 
                BentelerNumber, 
                Qty, 
                Date,
                DailyNotes,
                CONCAT(BentelerNumber, '_', CONVERT(VARCHAR, Date, 112)) as mrbID
            FROM dbo.InventoryCage 
            WHERE Disposition = 'Hold' AND inCage = 'Yes'
            ORDER BY Date ASC
            """
            df = self.execute_query(query)
            
            mrb_items = []
            for _, row in df.iterrows():
                mrb_id = str(row.get('mrbID', ''))
                
                # Calculate age
                date_value = row.get('Date')
                age_days = 0
                if pd.notna(date_value):
                    try:
                        date_obj = date_value.date() if hasattr(date_value, 'date') else datetime.strptime(str(date_value)[:10], '%Y-%m-%d').date()
                        age_days = (datetime.now().date() - date_obj).days
                    except:
                        age_days = 0
                
                # Access global
                global selected_mrb_parts
                
                mrb_items.append({
                    'mrbID': mrb_id,
                    'partNumber': str(row.get('BentelerNumber', '') if pd.notna(row.get('BentelerNumber')) else ''),
                    'partDescription': str(row.get('PartDescription', '') if pd.notna(row.get('PartDescription')) else ''),
                    'quantity': int(row.get('Qty', 0)) if pd.notna(row.get('Qty')) else 0,
                    'quarantineReason': str(row.get('Defect', '') if pd.notna(row.get('Defect')) else ''),
                    'ageInDays': age_days,
                    'note': str(row.get('DailyNotes', '') if pd.notna(row.get('DailyNotes')) else ''),
                    'isSelected': mrb_id in selected_mrb_parts
                })
            
            return mrb_items
            
        except Exception as e:
            logger.error(f"Error getting all MRB data: {str(e)}")
            raise

    def get_mrb_cage_items(self):
        """Get MRB cage items - FILTERED BY SELECTION"""
        try:
            # Get all items
            all_mrb_items = self.get_all_mrb_data()
            
            # Filter by selected items
            global selected_mrb_parts
            if selected_mrb_parts:
                filtered_items = [item for item in all_mrb_items 
                                if item.get('mrbID') in selected_mrb_parts]
            else:
                filtered_items = []
            
            return filtered_items
            
        except Exception as e:
            logger.error(f"Error getting MRB cage items: {str(e)}")
            return []
        
        


    @app.route('/api/quality/mrb-general-note', methods=['GET', 'POST'])
    def manage_mrb_general_note():
        """API endpoint to manage standalone general MRB note (stored in JSON file)"""
        global mrb_general_note
        
        if request.method == 'GET':
            try:
                return jsonify({
                    'note': mrb_general_note,
                    'success': True
                })
            except Exception as e:
                logger.error(f"Error getting MRB general note: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
        elif request.method == 'POST':
            try:
                data = request.get_json()
                general_note = data.get('generalNote', '').strip()
                
                save_mrb_general_note(general_note)
                
                return jsonify({
                    'success': True,
                    'message': 'General note saved successfully'
                })
                
            except Exception as e:
                logger.error(f"Error saving MRB general note: {str(e)}")
                return jsonify({'error': str(e)}), 500
        



    @app.route('/api/quality/mrb-all')
    def quality_mrb_all():
        """API endpoint for ALL MRB parts (for selection)"""
        if not quality_db:
            return jsonify({'error': 'Quality database not available'}), 503
        
        try:
            mrb_items = quality_db.get_all_mrb_data()
            return jsonify(mrb_items)
        except Exception as e:
            logger.error(f"API error in quality_mrb_all: {str(e)}")
            return jsonify({'error': str(e)}), 500

    @app.route('/api/quality/mrb-selection', methods=['POST'])
    def update_mrb_selections():
        """API endpoint to update which MRB parts to display with notes"""
        global selected_mrb_parts
        
        try:
            data = request.get_json()
            selected_items = data.get('selectedItems', [])
            
            # Update selections
            selected_ids = []
            
            # Get database connection
            conn = get_db_connection_1()  # G0X_Production
            cursor = conn.cursor()
            
            for item in selected_items:
                mrb_id = item.get('mrbID')
                is_selected = item.get('isSelected', False)
                note = item.get('note', '')
                
                if is_selected and mrb_id:
                    selected_ids.append(mrb_id)
                
                # Update note in database regardless of selection
                # Extract part number and date from mrbID
                try:
                    parts = mrb_id.split('_')
                    if len(parts) >= 2:
                        part_number = parts[0]
                        date_str = parts[1]
                        
                        # Convert date string (YYYYMMDD) to date
                        date_obj = datetime.strptime(date_str, '%Y%m%d').date()
                        
                        # Update the note in database using DailyNotes column
                        update_query = """
                        UPDATE dbo.InventoryCage 
                        SET DailyNotes = ?
                        WHERE BentelerNumber = ? 
                        AND CAST(Date AS DATE) = ?
                        AND Disposition = 'Hold' 
                        AND inCage = 'Yes'
                        """
                        cursor.execute(update_query, (note, part_number, date_obj))
                except Exception as e:
                    logger.warning(f"Could not update note for {mrb_id}: {str(e)}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            selected_mrb_parts = set(selected_ids)
            save_selected_mrb()
            
            return jsonify({
                'success': True,
                'message': f'Updated selection: {len(selected_ids)} MRB parts selected'
            })
            
        except Exception as e:
            logger.error(f"API error in update_mrb_selections: {str(e)}")
            return jsonify({'error': str(e)}), 500
        
    def get_sdr_summary(self):
        """Get SDR summary with Monday exception logic"""
        try:
            # Apply Monday exception logic
            today = datetime.now().weekday()  # Monday = 0, Sunday = 6
            if today == 0:  # Monday
                hours_back = -72  # Last 72 hours on Monday
            else:
                hours_back = -24  # Last 24 hours on other days
            
            conn = None
            try:
                conn = get_db_connection()  # Production database
                query = """
                SELECT COUNT(*) as count 
                FROM dbo.SupplierIssue 
                WHERE DateClosed >= DATEADD(hour, ?, GETDATE())
                """
                df = pd.read_sql(query, conn, params=[hours_back])
                
                sdr_count = int(df.iloc[0]['count']) if not df.empty else 0
                
                logger.info(f"SDR count retrieved: {sdr_count}")
                return sdr_count
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting SDR summary: {str(e)}")
            return 0

    def get_sdr_events(self, days=None):
        """Get SDR events with Monday exception logic"""
        try:
            # Apply Monday exception logic
            if days is None:
                today = datetime.now().weekday()  # Monday = 0, Sunday = 6
                if today == 0:  # Monday
                    hours_back = -72  # Last 72 hours on Monday
                else:
                    hours_back = -24  # Last 24 hours on other days
            else:
                hours_back = -24 * days
            
            conn = None
            try:
                conn = get_db_connection()  # Production database
                query = """
                SELECT 
                    Detail,
                    Quantity,
                    Line,
                    DateClosed
                FROM dbo.SupplierIssue 
                WHERE DateClosed >= DATEADD(hour, ?, GETDATE())
                ORDER BY DateClosed DESC
                """
                df = pd.read_sql(query, conn, params=[hours_back])
                
                events = []
                for _, row in df.iterrows():
                    # Convert datetime properly
                    date_closed = row.get('DateClosed')
                    if pd.notna(date_closed):
                        if hasattr(date_closed, 'isoformat'):
                            date_closed_str = date_closed.isoformat()
                        else:
                            date_closed_str = str(date_closed)
                    else:
                        date_closed_str = 'N/A'
                    
                    events.append({
                        'detail': str(row.get('Detail', '') if pd.notna(row.get('Detail')) else ''),
                        'quantity': int(row.get('Quantity', 0)) if pd.notna(row.get('Quantity')) else 0,
                        'line': str(row.get('Line', '') if pd.notna(row.get('Line')) else ''),
                        'dateClosed': date_closed_str
                    })
                
                return events
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting SDR events: {str(e)}")
            return []
    


    def get_bmw_quality_claims(self, hours=24):
        """Get BMW/VW quality claims from last N hours"""
        try:
            query = """
            SELECT * FROM dbo.DailyBMWQualityClaims 
            WHERE dateTime >= DATEADD(hour, ?, GETDATE())
            ORDER BY dateTime DESC
            """
            df = self.execute_query(query, params=[-hours])
            
            claims = []
            for _, row in df.iterrows():
                # Convert datetime properly
                date_time = row.get('dateTime')
                if pd.notna(date_time):
                    if hasattr(date_time, 'isoformat'):
                        reported_date = date_time.isoformat()
                    else:
                        reported_date = str(date_time)
                else:
                    reported_date = 'N/A'
                
                claims.append({
                    'claimID': str(row.get('claimID', '')),
                    'reportedDate': reported_date,
                    'customer': 'BMW/VW',
                    'event': str(row.get('event', '') if pd.notna(row.get('event')) else ''),
                    'responsible': str(row.get('responsible', '') if pd.notna(row.get('responsible')) else ''),
                    'actionTaken': str(row.get('actionTaken', '') if pd.notna(row.get('actionTaken')) else ''),
                    'severityBI': str(row.get('severityBI', '') if pd.notna(row.get('severityBI')) else 'B'),
                    'owner': str(row.get('owner', '') if pd.notna(row.get('owner')) else ''),
                    'description': str(row.get('event', '') if pd.notna(row.get('event')) else ''),
                    'partNumber': 'N/A',
                    'severity': str(row.get('severityBI', 'Medium') if pd.notna(row.get('severityBI')) else 'Medium'),
                    'status': 'Closed' if pd.notna(row.get('actionTaken')) and str(row.get('actionTaken')).strip() else 'Open'
                })
            
            return claims
            
        except Exception as e:
            logger.error(f"Error getting BMW quality claims: {str(e)}")
            raise

    def get_key_turns(self, hours=48):
        """Get key turns from last N hours"""
        try:
            query = """
            SELECT * FROM dbo.DailyKeyTurns 
            WHERE dateTime >= DATEADD(hour, ?, GETDATE())
            ORDER BY dateTime DESC
            """
            df = self.execute_query(query, params=[-hours])
            
            key_turns = []
            for _, row in df.iterrows():
                # Convert datetime properly
                date_time = row.get('dateTime')
                if pd.notna(date_time):
                    if hasattr(date_time, 'isoformat'):
                        timestamp = date_time.isoformat()
                    else:
                        timestamp = str(date_time)
                else:
                    timestamp = 'N/A'
                
                # Make sure orderNumber is explicitly included
                order_number = str(row.get('orderNumber', '') if pd.notna(row.get('orderNumber')) else '')
                
                key_turns.append({
                    'keyTurnID': str(row.get('keyTurnID', '')),
                    'timestamp': timestamp,
                    'keyType': str(row.get('event', '') if pd.notna(row.get('event')) else ''),
                    'station': 'Production Line',
                    'partNumber': order_number,  # For backward compatibility
                    'operator': 'N/A',
                    'reason': str(row.get('description', '') if pd.notna(row.get('description')) else ''),
                    'description': str(row.get('description', '') if pd.notna(row.get('description')) else ''),
                    'orderNumber': order_number  # EXPLICITLY ADD THIS
                })
            
            return key_turns
            
        except Exception as e:
            logger.error(f"Error getting key turns: {str(e)}")
            raise

    def get_eol_findings(self, hours=24):
        """Get EOL findings from G0X_Production.dbo.QualityBMWLOG with CostCenterID"""
        conn = None
        try:
            conn = get_db_connection()  # Use production database directly

            query = f"""
            SELECT 
                bmw.description,
                bmw.OrderNumber,
                bmw.qualityEOLID,
                bmw.dateRecorded,
                bmw.component,
                bmw.defect,
                bmw.TL_A,
                bmw.TL_A_Time,
                bmw.side,
                bmw.rework,
                bmw.E_Button,
                bmw.Ecart,
                eol.costCenterID
            FROM dbo.QualityBMWLOG bmw
            LEFT JOIN dbo.QualityEOLMainReport eol 
                ON bmw.qualityEOLID = eol.qualityEOLID
            INNER JOIN dbo.main main
                ON main.qualityEOLID = bmw.qualityEOLID
            WHERE bmw.dateRecorded >= DATEADD(hour, ?, GETDATE())
            AND main.closed = 'Yes'
            ORDER BY bmw.dateRecorded DESC
            """

            df = pd.read_sql(query, conn, params=[-hours])
            
            findings = []
            for _, row in df.iterrows():
                # Convert datetime properly
                date_recorded = row.get('dateRecorded')
                if pd.notna(date_recorded):
                    if hasattr(date_recorded, 'isoformat'):
                        recorded_date = date_recorded.isoformat()
                    else:
                        recorded_date = str(date_recorded)
                else:
                    recorded_date = 'N/A'
                
                # Map costCenterID to Front/Rear
                cost_center_id = row.get('costCenterID')
                cost_center_location = 'N/A'
                if pd.notna(cost_center_id):
                    if cost_center_id == 1:
                        cost_center_location = 'Front'
                    elif cost_center_id == 2:
                        cost_center_location = 'Rear'
                
                findings.append({
                    'qualityEOLID': str(row.get('qualityEOLID', '')),
                    'dateRecorded': recorded_date,
                    'description': str(row.get('description', '') if pd.notna(row.get('description')) else ''),
                    'orderNumber': str(row.get('OrderNumber', '') if pd.notna(row.get('OrderNumber')) else ''),
                    'ecartNumber': str(row.get('Ecart', '') if pd.notna(row.get('Ecart')) else ''),
                    'eButton': str(row.get('E_Button', '') if pd.notna(row.get('E_Button')) else ''),
                    'side': str(row.get('side', '') if pd.notna(row.get('side')) else ''),
                    'component': str(row.get('component', '') if pd.notna(row.get('component')) else ''),
                    'defect': str(row.get('defect', '') if pd.notna(row.get('defect')) else ''),
                    'costCenterLocation': cost_center_location  # Add this new field
                })
            
            return findings
            
        except Exception as e:
            logger.error(f"Error getting EOL findings from G0X_Production: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()


    def get_active_quality_alerts(self):
        """Get active quality alerts from G0X_Production.dbo.QualityAlert_Main where FechaFinal > today"""
        conn = None
        try:
            conn = get_db_connection()  # Use production database directly
            query = """
            SELECT 
                ID_QA, 
                Defect, 
                FechaFinal,
                QualityAuthorization,
                ProductionAuthorization,
                CreatedBy,
                WhenF as CreatedDate
            FROM dbo.QualityAlert_Main 
            WHERE FromDate >= DATEADD(DAY, 
                CASE 
                    WHEN DATEPART(WEEKDAY, GETDATE()) = 2 THEN -4  
                    ELSE -2  
                END, 
                GETDATE())            
                ORDER BY FechaFinal DESC
            """
            df = pd.read_sql(query, conn)
            
            alerts = []
            for _, row in df.iterrows():
                # Convert datetime properly
                fecha_final = row.get('FechaFinal')
                created_date = row.get('CreatedDate')
                
                if pd.notna(fecha_final):
                    if hasattr(fecha_final, 'isoformat'):
                        final_date = fecha_final.isoformat()
                    else:
                        final_date = str(fecha_final)
                else:
                    final_date = 'N/A'
                
                if pd.notna(created_date):
                    if hasattr(created_date, 'isoformat'):
                        created_date_str = created_date.isoformat()
                    else:
                        created_date_str = str(created_date)
                else:
                    created_date_str = 'N/A'
                
                alerts.append({
                    'idQA': str(row.get('ID_QA', '')),
                    'defect': str(row.get('Defect', '') if pd.notna(row.get('Defect')) else ''),
                    'fechaFinal': final_date,
                    'alertType': 'Active Quality Alert',
                    'status': 'Active',
                    'qualityAuth': str(row.get('QualityAuthorization', '') if pd.notna(row.get('QualityAuthorization')) else ''),
                    'QualityAuthorization': str(row.get('QualityAuthorization', '') if pd.notna(row.get('QualityAuthorization')) else ''),
                    'createdBy': str(row.get('CreatedBy', '') if pd.notna(row.get('CreatedBy')) else ''),
                    'createdDate': created_date_str
                })
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error getting active quality alerts from G0X_Production: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    def get_scrap_data(self, hours=72):
        """Get scrap data from G0X_Production.dbo.QualityModuleTech joined with Quality_MainReport and Quality_Material"""
        conn = None
        try:
            conn = get_db_connection()  # Use production database directly
            query = """
            SELECT 
                qmt.qualityID,
                qmt.comment,
                qmat.Description as materialDescription,  -- Get Description from Quality_Material
                qmt.QualityMainID,
                qmr.date,
                qmr.qualityMainID as mainReportID
            FROM dbo.QualityModuleTech qmt
            INNER JOIN dbo.Quality_MainReport qmr ON qmt.QualityMainID = qmr.qualityMainID
            LEFT JOIN dbo.Quality_Material qmat ON qmt.materialID = qmat.BentelerNumber  -- FIXED: Use qmt.materialID instead of qmr.materialID
            WHERE qmr.date >= DATEADD(hour, ?, GETDATE())
            ORDER BY qmr.date DESC
            """
            df = pd.read_sql(query, conn, params=[-hours])
            
            scrap_items = []
            for _, row in df.iterrows():
                quality_id = str(row.get('qualityID', ''))
                quality_main_id = str(row.get('QualityMainID', ''))
                date_recorded = row.get('date')
                if pd.notna(date_recorded) and hasattr(date_recorded, 'isoformat'):
                    recorded_date = date_recorded.isoformat()
                else:
                    recorded_date = str(date_recorded) if date_recorded else 'N/A'
                
                # Access the global selected_scrap_reports variable
                global selected_scrap_reports
                
                scrap_items.append({
                    'qualityID': quality_id,
                    'qualityMainID': quality_main_id,
                    'mainReportID': str(row.get('mainReportID', '')),
                    'date': recorded_date,
                    'comment': str(row.get('comment', '') if pd.notna(row.get('comment')) else ''),
                    'why': str(row.get('materialDescription', '') if pd.notna(row.get('materialDescription')) else ''),  # From Quality_Material.Description
                    'isSelected': quality_id in selected_scrap_reports
                })
            
            return scrap_items
            
        except Exception as e:
            logger.error(f"Error getting scrap data from G0X_Production: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

        



    def get_line_quality_issues(self, hours=48):
        """Get line quality issues from last N hours"""
        try:
            query = """
            SELECT * FROM dbo.DailyLineQuality 
            WHERE dateTime >= DATEADD(hour, ?, GETDATE())
            ORDER BY dateTime DESC
            """
            df = self.execute_query(query, params=[-hours])
            
            issues = []
            for _, row in df.iterrows():
                # Convert datetime properly
                date_time = row.get('dateTime')
                if pd.notna(date_time):
                    if hasattr(date_time, 'isoformat'):
                        created_date = date_time.isoformat()
                    else:
                        created_date = str(date_time)
                else:
                    created_date = 'N/A'
                
                # Handle line number (it's an int in your DB)
                line_number = row.get('line', 'N/A')
                if pd.notna(line_number):
                    line_display = f"Line {int(line_number)}"
                else:
                    line_display = "Line N/A"
                
                issues.append({
                    'lineQualityID': str(row.get('lineQualityID', '')),
                    'createdDate': created_date,
                    'alertType': 'EOL Quality Issue',
                    'station': line_display,
                    'description': str(row.get('descriptionOfIssue', '') if pd.notna(row.get('descriptionOfIssue')) else ''),
                    'identifiedBy': str(row.get('identifiedBy', '') if pd.notna(row.get('identifiedBy')) else ''),
                    'actionTaken': str(row.get('actionTaken', '') if pd.notna(row.get('actionTaken')) else ''),
                    'owner': str(row.get('owner', '') if pd.notna(row.get('owner')) else ''),
                    'severity': str(row.get('pdcaYN', 'Medium') if pd.notna(row.get('pdcaYN')) else 'Medium'),
                    'assignedTo': str(row.get('owner', '') if pd.notna(row.get('owner')) else '')
                })
            
            return issues
            
        except Exception as e:
            logger.error(f"Error getting line quality issues: {str(e)}")
            raise


    def get_supplier_by_id_with_logo(self, supplier_id):
        """Get a specific supplier with logo information"""
        try:
            conn = None
            try:
                conn = get_db_connection_1()  # Test database (G0X_Production_Test)
                query = "SELECT ID, Name FROM dbo.Logistics_Planner WHERE ID = ?"
                df = pd.read_sql(query, conn, params=[supplier_id])
                
                if df.empty:
                    return None
                
                row = df.iloc[0]
                return {
                    'id': str(row.get('ID', '')),
                    'name': str(row.get('Name', '') if pd.notna(row.get('Name')) else ''),
                    'logoPath': f"/static/vendor-logos/{row.get('ID', '')}.png"
                }
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting supplier by ID with logo: {str(e)}")
            return None

    # Add these new API endpoints to your app.py

    @app.route('/api/suppliers/with-logos')
    def suppliers_with_logos():
        """API endpoint for suppliers with logo information"""
        if not logistics_db:
            return jsonify({'error': 'Logistics database not available'}), 503
        
        try:
            suppliers = logistics_db.get_suppliers_with_logos()
            return jsonify(suppliers)
        except Exception as e:
            logger.error(f"API error in suppliers_with_logos: {str(e)}")
            return jsonify({'error': str(e)}), 500

    @app.route('/static/vendor-logos/<filename>')
    def serve_vendor_logo(filename):
        """Serve vendor logo files"""
        try:
            # Validate filename to prevent directory traversal
            if not filename.endswith('.png') or '/' in filename or '\\' in filename:
                return jsonify({'error': 'Invalid filename'}), 400
            
            vendor_logos_path = os.path.join(APP_DIR, 'static', 'vendor-logos')
            
            # Check if file exists
            file_path = os.path.join(vendor_logos_path, filename)
            if not os.path.exists(file_path):
                # Return a default "no logo" image or 404
                return jsonify({'error': 'Logo not found'}), 404
            
            return send_from_directory(vendor_logos_path, filename)
            
        except Exception as e:
            logger.error(f"Error serving vendor logo {filename}: {str(e)}")
            return jsonify({'error': 'Logo not available'}), 404

    # Update the QualityDatabase class methods to include supplier logo information

    def get_supplier_containments(self):
        """Get active supplier containment actions with supplier logo support"""
        try:
            query = """
                SELECT *, GETDATE() as createdDate 
                FROM dbo.DailySupplierContainment 
                ORDER BY containmentID DESC

            """
            df = self.execute_query(query)
            
            containments = []
            for _, row in df.iterrows():
                # Determine risk level based on parts inspected and defect rate
                total_parts = row.get('totalPartsInspected', 0)
                defect_parts = row.get('totalDefectParts', 0)
                defect_rate = row.get('defectRate', 0)
                
                # Handle potential NaN values
                if pd.notna(total_parts):
                    total_parts = int(total_parts)
                else:
                    total_parts = 0
                    
                if pd.notna(defect_parts):
                    defect_parts = int(defect_parts)
                else:
                    defect_parts = 0
                    
                if pd.notna(defect_rate):
                    defect_rate = float(defect_rate)
                else:
                    defect_rate = 0.0
                
                risk_level = 'Low'
                if defect_rate > 3.0 or defect_parts > 100:
                    risk_level = 'High'
                elif defect_rate > 1.0 or defect_parts > 50:
                    risk_level = 'Medium'

                # Get supplier name - try both possible columns
                supplier_name = str(row.get('supplierName', '') if pd.notna(row.get('supplierName')) else '')
                if not supplier_name:
                    supplier_name = str(row.get('supplier', '') if pd.notna(row.get('supplier')) else '')
                
                # Initialize logo variables
                supplier_logo_id = None
                supplier_logo_path = None
                
                # Try to find logo information by querying TEST database for Logistics_Planner
                if supplier_name and supplier_name.strip():
                    try:
                        # Query TEST database for supplier logo using EXACT name match first
                        conn_test = get_db_connection_1()  # Test database (G0X_Production_Test)
                        cursor = conn_test.cursor()
                        
                        # Try exact match first
                        logo_query = "SELECT VendorID FROM dbo.Logistics_Planner WHERE Vendor = ?"
                        cursor.execute(logo_query, (supplier_name.strip(),))
                        logo_result = cursor.fetchone()
                        
                        if not logo_result:
                            # Try partial match if exact fails
                            logo_query = "SELECT VendorID FROM dbo.Logistics_Planner WHERE Vendor LIKE ?"
                            cursor.execute(logo_query, (f'%{supplier_name.strip()}%',))
                            logo_result = cursor.fetchone()
                        
                        if logo_result:
                            supplier_logo_id = str(logo_result[0])
                            supplier_logo_path = f"/static/vendor-logos/{supplier_logo_id}.png"
                            logger.info(f"Found logo for supplier '{supplier_name}': ID={supplier_logo_id}")
                        else:
                            logger.info(f"No logo found for supplier: '{supplier_name}'")
                        
                        cursor.close()
                        conn_test.close()
                        
                    except Exception as logo_error:
                        logger.warning(f"Could not fetch supplier logo for '{supplier_name}': {str(logo_error)}")
                
                containments.append({
                    'containmentID': str(row.get('containmentID', '')),
                    'createdDate': 'N/A',  # No dateTime column in this table
                    'supplierName': supplier_name,
                    'supplier': supplier_name,  # Keep original field for compatibility
                    'supplierLogoId': supplier_logo_id,
                    'supplierLogoPath': supplier_logo_path,
                    'partNumber': 'N/A',
                    'issueDescription': str(row.get('nonConformance', '') if pd.notna(row.get('nonConformance')) else ''),
                    'nonConformance': str(row.get('nonConformance', '') if pd.notna(row.get('nonConformance')) else ''),
                    'actionTaken': str(row.get('actionTaken', '') if pd.notna(row.get('actionTaken')) else ''),
                    'statusUpdate': str(row.get('statusUpdate', '') if pd.notna(row.get('statusUpdate')) else ''),
                    'owner': str(row.get('owner', '') if pd.notna(row.get('owner')) else ''),
                    'totalPartsInspected': total_parts,
                    'totalDefectParts': defect_parts,
                    'defectRate': defect_rate,
                    'riskLevel': risk_level,
                    'assignedTo': str(row.get('owner', '') if pd.notna(row.get('owner')) else '')
                })
            
            logger.info(f"Retrieved {len(containments)} supplier containments with logo support")
            return containments
            
        except Exception as e:
            logger.error(f"Error getting supplier containments: {str(e)}")
            raise

    # Add CRUD methods inside the class
    def add_bmw_claim(self, claim_data):
        """Add a new BMW/VW quality claim"""
        try:
            query = """
            INSERT INTO dbo.DailyBMWQualityClaims (
                dateTime, event, responsible, actionTaken, severityBI, owner
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
            
            params = [
                claim_data.get('dateTime'),
                claim_data.get('event', ''),
                claim_data.get('responsible', ''),
                claim_data.get('actionTaken', ''),
                claim_data.get('severityBI', 'B'),
                claim_data.get('owner', '')
            ]
            
            rows_affected = self.execute_non_query(query, params)
            logger.info(f"Added BMW claim, rows affected: {rows_affected}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding BMW claim: {str(e)}")
            raise

    def update_bmw_claim(self, claim_id, claim_data):
        """Update an existing BMW/VW quality claim"""
        try:
            query = """
            UPDATE dbo.DailyBMWQualityClaims SET 
                dateTime = ?, event = ?, responsible = ?, actionTaken = ?, severityBI = ?, owner = ?
            WHERE claimID = ?
            """
            
            params = [
                claim_data.get('dateTime'),
                claim_data.get('event', ''),
                claim_data.get('responsible', ''),
                claim_data.get('actionTaken', ''),
                claim_data.get('severityBI', 'B'),
                claim_data.get('owner', ''),
                claim_id
            ]
            
            rows_affected = self.execute_non_query(query, params)
            logger.info(f"Updated BMW claim {claim_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error updating BMW claim: {str(e)}")
            raise

    def delete_bmw_claim(self, claim_id):
        """Delete a BMW/VW quality claim"""
        try:
            query = "DELETE FROM dbo.DailyBMWQualityClaims WHERE claimID = ?"
            rows_affected = self.execute_non_query(query, [claim_id])
            logger.info(f"Deleted BMW claim {claim_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error deleting BMW claim: {str(e)}")
            raise

    def get_bmw_claim_by_id(self, claim_id):
        """Get a specific BMW claim by ID"""
        try:
            query = "SELECT * FROM dbo.DailyBMWQualityClaims WHERE claimID = ?"
            df = self.execute_query(query, params=[claim_id])
            
            if df.empty:
                return None
            
            claim = df.iloc[0].to_dict()
            
            # Convert to expected format
            for key, value in claim.items():
                if pd.isna(value):
                    claim[key] = None
                elif hasattr(value, 'isoformat'):
                    claim[key] = value.isoformat()
                else:
                    claim[key] = str(value) if value is not None else ''
            
            return claim
            
        except Exception as e:
            logger.error(f"Error getting BMW claim by ID: {str(e)}")
            raise

# Initialize database connections
try:
    safety_db = SafetyDatabase()
except Exception as e:
    logger.critical(f"Failed to initialize safety database: {str(e)}")
    safety_db = None

try:
    quality_db = QualityDatabase()
    logger.info("Quality database initialized successfully")
except Exception as e:
    logger.critical(f"Failed to initialize quality database: {str(e)}")
    quality_db = None

# Global variable to store selected scrap reports
selected_scrap_reports = set()

def load_selected_scraps():
    """Load selected scrap IDs from file"""
    global selected_scrap_reports
    try:
        import json
        with open('selected_scraps.json', 'r') as f:
            selected_scrap_reports = set(json.load(f))
    except FileNotFoundError:
        selected_scrap_reports = set()

def save_selected_scraps():
    """Save selected scrap IDs to file"""
    import json
    with open('selected_scraps.json', 'w') as f:
        json.dump(list(selected_scrap_reports), f)

# Call this when app starts
load_selected_scraps()

# Global variable for MRB selections
selected_mrb_parts = set()

def load_selected_mrb():
    """Load selected MRB IDs from file"""
    global selected_mrb_parts
    try:
        import json
        with open('selected_mrb.json', 'r') as f:
            selected_mrb_parts = set(json.load(f))
    except FileNotFoundError:
        selected_mrb_parts = set()

def save_selected_mrb():
    """Save selected MRB IDs to file"""
    import json
    with open('selected_mrb.json', 'w') as f:
        json.dump(list(selected_mrb_parts), f)

# Call when app starts
load_selected_mrb()

@app.route('/api/quality/scrap-all')
def quality_scrap_all():
    """API endpoint for ALL scrap reports (for selection)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        hours = request.args.get('hours', 72, type=int)
        scrap_items = quality_db.get_all_scrap_data(hours)
        return jsonify(scrap_items)
    except Exception as e:
        logger.error(f"API error in quality_scrap_all: {str(e)}")
        return jsonify({'error': str(e)}), 500


    
@app.route('/api/quality/scrap-selection', methods=['POST'])
def update_scrap_selections():
    """API endpoint to update which scrap reports to display"""
    global selected_scrap_reports
    
    try:
        data = request.get_json()
        selected_ids = data.get('selectedIds', [])
        
        selected_scrap_reports = set(selected_ids)
        save_selected_scraps()
        
        return jsonify({
            'success': True,
            'message': f'Updated selection: {len(selected_ids)} reports selected'
        })
        
    except Exception as e:
        logger.error(f"API error in update_scrap_selections: {str(e)}")
        return jsonify({'error': str(e)}), 500
    


# Page Routes
@app.route('/safety')
def dashboard():
    """Main dashboard page"""
    return render_template('safety.html')

@app.route('/quality')
def quality_page():
    """Quality dashboard page"""
    return render_template('quality.html')

@app.route('/incidents')
@require_incidents_access
def incidents_page():
    """Incidents management page"""
    return render_template('incidents.html')

@app.route('/quality-management')
@require_quality_access
def quality_management_page():
    """Quality management page"""
    return render_template('quality_management.html')

# Safety API Routes
@app.route('/api/safety/summary')
def safety_summary():
    """API endpoint for safety summary"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        summary = safety_db.get_safety_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in safety_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/safety/by-type')
def incidents_by_type():
    """API endpoint for incidents by type"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        days = request.args.get('days', type=int)
        data = safety_db.get_incident_by_type(days)
        return jsonify(data)
    except Exception as e:
        logger.error(f"API error in incidents_by_type: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/safety/trend')
def incident_trend():
    """API endpoint for incident trend"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        days = request.args.get('days', 30, type=int)
        data = safety_db.get_daily_trend(days)
        return jsonify(data)
    except Exception as e:
        logger.error(f"API error in incident_trend: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/safety/recent')
def recent_incidents():
    """API endpoint for recent incidents with Monday exception"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        days_param = request.args.get('days', type=int)
        
        if days_param is None:
            today = datetime.now().weekday()
            if today == 0:
                days = 3
            else:
                days = 1
        else:
            days = days_param
        
        if days == 1:
            df = safety_db.get_recent_incidents(1)
        else:
            df = safety_db.get_recent_incidents(days)
        
        incidents = []
        for _, row in df.iterrows():
            # CRITICAL FIX: Handle time from both databases
            time_value = row.get('time') or row.get('timeEvent') or row.get('dateEP')
            
            if pd.notna(time_value):
                if hasattr(time_value, 'isoformat'):
                    time_str = time_value.isoformat()
                else:
                    time_str = str(time_value)
            else:
                time_str = datetime.now().isoformat()
                logger.warning(f"No time for incident {row.get('sheID')}")
            
            incidents.append({
                'id': str(row.get('sheID', '')),
                'time': time_str,
                'incidentType': str(row.get('incidentType', '')),
                'description': str(row.get('description', '')),
                'machineID': str(row.get('machineID', '') or row.get('operationID', '')),
                'immediateAction': str(row.get('inmediateAction', '') or row.get('immediateAction', '')),
                'personalNumber': str(row.get('personalNumber', '')),
                'reportedBy': str(row.get('reportedBy', '')),
                'severity': 'Medium',
                'status': 'Open',
                'location': str(row.get('xWhere', '')),
                'mainID': str(row.get('mainID', '')),
                'source': str(row.get('source', 'Unknown'))
            })
        
        logger.info(f"Returning {len(incidents)} recent incidents")
        return jsonify(incidents)
        
    except Exception as e:
        logger.error(f"API error in recent_incidents: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/machines/active')
def active_machines():
    """API endpoint for active machines"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        machines = safety_db.get_active_machines()
        return jsonify(machines)
    except Exception as e:
        logger.error(f"API error in active_machines: {str(e)}")
        return jsonify({'error': str(e)}), 500

# FIXED VERSION - Add this POST handler to your manage_incidents route

@app.route('/api/safety/incidents', methods=['GET', 'POST'])
def manage_incidents():
    """API endpoint for managing incidents - GET all or POST new"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    if request.method == 'GET':
        try:
            df = safety_db.get_all_safety_data()
            
            incidents = []
            for _, row in df.iterrows():
                # CRITICAL FIX: Handle time from both G0X and WELD
                # Try multiple time fields and ensure we always have a time value
                time_value = row.get('time') or row.get('timeEvent') or row.get('dateEP')
                
                if pd.notna(time_value):
                    if hasattr(time_value, 'isoformat'):
                        time_str = time_value.isoformat()
                    else:
                        time_str = str(time_value)
                else:
                    # Fallback to current time if no time field exists
                    time_str = datetime.now().isoformat()
                    logger.warning(f"No time found for incident {row.get('sheID', 'unknown')}, using current time")
                
                incidents.append({
                    'id': str(row.get('sheID', '')),
                    'time': time_str,  # PRIMARY time field
                    'dateEP': time_str,  # ALSO include as dateEP for frontend compatibility
                    'incidentType': str(row.get('incidentType', '')),
                    'description': str(row.get('description', '')),
                    'machineID': str(row.get('machineID', '') or row.get('operationID', '')),
                    'immediateAction': str(row.get('xWhat', '') or row.get('immediateAction', '')),
                    'reportedBy': str(row.get('personalNumber', '')),
                    'severity': 'Medium',
                    'status': 'Open',
                    'location': str(row.get('xWhere', '')),
                    'mainID': str(row.get('mainID', '')),
                    'source': str(row.get('source', 'Unknown'))
                })
            
            logger.info(f"Returning {len(incidents)} incidents to frontend")
            return jsonify(incidents)
            
        except Exception as e:
            logger.error(f"API error in get incidents: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'POST':
        try:
            incident_data = request.get_json()
            
            # Validate required fields
            required_fields = ['mainID', 'time', 'incidentType', 'description', 'personalNumber']
            missing_fields = [field for field in required_fields if not incident_data.get(field)]
            
            if missing_fields:
                return jsonify({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                }), 400
            
            # Prepare data for database insertion
            filtered_data = {
                'mainID': incident_data.get('mainID'),
                'time': incident_data.get('time'),
                'incidentType': incident_data.get('incidentType'),
                'machineID': incident_data.get('machineID', ''),
                'description': incident_data.get('description'),
                'inmediateAction': incident_data.get('inmediateAction', ''),
                'personalNumber': incident_data.get('personalNumber'),
                'location': incident_data.get('location', ''),
            }
            
            # Parse datetime if needed
            if 'time' in filtered_data and filtered_data['time']:
                try:
                    time_str = filtered_data['time']
                    if 'T' in time_str:
                        incident_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    else:
                        incident_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                    
                    filtered_data['time'] = incident_time
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format: {str(ve)}")
                    return jsonify({
                        'error': 'Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS or ISO format.'
                    }), 400
            
            logger.info(f"Creating new incident with data: {filtered_data}")
            
            # Insert the incident
            success = safety_db.add_incident(filtered_data)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Incident created successfully'
                }), 201
            else:
                return jsonify({
                    'error': 'Failed to create incident'
                }), 500
                
        except Exception as e:
            logger.error(f"API error in create incident: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    

@app.route('/api/safety/incidents/<incident_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_incident(incident_id):
    """API endpoint for managing a single incident"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    if request.method == 'GET':
        try:
            incident = safety_db.get_incident_by_id(incident_id)
            if not incident:
                return jsonify({'error': 'Incident not found'}), 404
            
            if 'time' in incident:
                incident['time'] = str(incident['time'])
            
            return jsonify(incident)
        except Exception as e:
            logger.error(f"API error in get incident: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            incident_data = request.get_json()
            
            filtered_data = {
                'time': incident_data.get('time'),
                'incidentType': incident_data.get('incidentType'),
                'machineID': incident_data.get('machineID'),
                'description': incident_data.get('description'),
                'inmediateAction': incident_data.get('inmediateAction'),
                'personalNumber': incident_data.get('personalNumber'),
                'location': incident_data.get('location', ''),
            }
            
            if 'time' in filtered_data and filtered_data['time']:
                try:
                    time_str = filtered_data['time']
                    if 'T' in time_str:
                        incident_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    else:
                        incident_time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                    
                    filtered_data['time'] = incident_time
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format: {str(ve)}")
                    return jsonify({'error': 'Invalid datetime format. Please use YYYY-MM-DD HH:MM:SS or ISO format.'}), 400
            
            logger.info(f"Updating incident {incident_id} with filtered data: {filtered_data}")
            
            success = safety_db.update_incident(incident_id, filtered_data)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Incident updated successfully'
                })
            else:
                return jsonify({'error': 'Incident not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update incident: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            success = safety_db.delete_incident(incident_id)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Incident deleted successfully'
                })
            else:
                return jsonify({'error': 'Incident not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete incident: {str(e)}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/associates/active')
def active_associates():
    """API endpoint for active associates"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        associates = safety_db.get_active_associates()
        return jsonify(associates)
    except Exception as e:
        logger.error(f"API error in active_associates: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/main/recent')
def recent_main_records():
    """API endpoint for recent main records"""
    if not safety_db:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        limit = request.args.get('limit', 2, type=int)
        records = safety_db.get_recent_main_records(limit)
        return jsonify(records)
    except Exception as e:
        logger.error(f"API error in recent_main_records: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        if safety_db:
            conn = safety_db.get_connection()
            conn.close()
            return jsonify({'status': 'healthy', 'database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 503
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

# Quality API Routes
@app.route('/api/quality/summary')
def quality_summary():
    """API endpoint for quality summary"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        summary = quality_db.get_quality_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in quality_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

# CORRECTED: New endpoint for recent claims (for expandable metric cards)
@app.route('/api/quality/claims/recent')
def quality_claims_recent():
    """API endpoint for recent quality claims (for expandable cards)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        claims = quality_db.get_bmw_quality_claims(24)  # Last 48 hours
        return jsonify(claims)
    except Exception as e:
        logger.error(f"API error in quality_claims_recent: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/claims')
def quality_claims():
    """API endpoint for BMW/VW quality claims"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        hours = request.args.get('hours', 24, type=int)
        claims = quality_db.get_bmw_quality_claims(hours)
        return jsonify(claims)
    except Exception as e:
        logger.error(f"API error in quality_claims: {str(e)}")
        return jsonify({'error': str(e)}), 500

# CORRECTED: New endpoint for recent key turns
@app.route('/api/quality/key-turns/recent')
def quality_key_turns_recent():
    """API endpoint for recent key turns (for expandable cards)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        # Monday exception: 72 hours on Monday, 24 hours on other days
        today = datetime.now().weekday()  # Monday = 0, Sunday = 6
        hours = 72 if today == 0 else 24
        key_turns = quality_db.get_key_turns(hours)
        return jsonify(key_turns)
    except Exception as e:
        logger.error(f"API error in quality_key_turns_recent: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/key-turns')
def quality_key_turns():
    """API endpoint for key turns"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        hours = request.args.get('hours', 24, type=int)
        key_turns = quality_db.get_key_turns(hours)
        return jsonify(key_turns)
    except Exception as e:
        logger.error(f"API error in quality_key_turns: {str(e)}")
        return jsonify({'error': str(e)}), 500

# CORRECTED: New endpoint for recent alerts
@app.route('/api/quality/alerts/recent')
def quality_alerts_recent():
    """API endpoint for recent quality alerts (for expandable cards)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        alerts = quality_db.get_line_quality_issues(24)
        return jsonify(alerts)
    except Exception as e:
        logger.error(f"API error in quality_alerts_recent: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/alerts')
def quality_alerts():
    """API endpoint for quality alerts (line quality issues)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        hours = request.args.get('hours', 24, type=int)
        alerts = quality_db.get_line_quality_issues(hours)
        return jsonify(alerts)
    except Exception as e:
        logger.error(f"API error in quality_alerts: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/quality/scrap-review')
def quality_scrap_review():
    """API endpoint for scrap review"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        hours = request.args.get('hours', 72, type=int)
        scrap_items = quality_db.get_scrap_data(hours)
        return jsonify(scrap_items)
    except Exception as e:
        logger.error(f"API error in quality_scrap_review: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/scrap-review/recent')
def quality_scrap_review_recent():
    """API endpoint for recent scrap review (for expandable cards) - FILTERED BY SELECTION"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        # Get all scrap items
        all_scrap_items = quality_db.get_scrap_data(72)  # Last 24 hours
        
        # Filter by selected items
        global selected_scrap_reports
        if selected_scrap_reports:
            # Only show selected items
            filtered_items = [item for item in all_scrap_items 
                            if item.get('qualityID') in selected_scrap_reports]
        else:
            # If nothing is selected, show all items (or empty - your choice)
            filtered_items = all_scrap_items
        
        return jsonify(filtered_items)
    except Exception as e:
        logger.error(f"API error in quality_scrap_review_recent: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/supplier-containment')
def quality_supplier_containment():
    """API endpoint for supplier containment actions"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        containments = quality_db.get_supplier_containments()
        return jsonify(containments)
    except Exception as e:
        logger.error(f"API error in quality_supplier_containment: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/mrb-cage')
def quality_mrb_cage():
    """API endpoint for MRB cage items"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        mrb_items = quality_db.get_mrb_cage_items()
        return jsonify(mrb_items)
    except Exception as e:
        logger.error(f"API error in quality_mrb_cage: {str(e)}")
        return jsonify({'error': str(e)}), 500
    



@app.route('/api/quality/health')
def quality_health_check():
    """Quality system health check endpoint"""
    try:
        if quality_db:
            conn = quality_db.get_connection()
            conn.close()
            return jsonify({'status': 'healthy', 'quality_database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'quality_database': 'disconnected'}), 503
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

@app.route('/api/quality/debug-columns')
def debug_quality_columns():
    """Debug endpoint to check actual column names"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        tables_info = {}
        
        for table in ['DailyPDCACards', 'DailySupplierContainment', 'DailyBMWQualityClaims', 'DailyKeyTurns', 'DailyLineQuality']:
            try:
                columns = quality_db.debug_table_columns(table)
                tables_info[table] = columns
            except Exception as e:
                tables_info[table] = f"Error: {str(e)}"
        
        return jsonify(tables_info)
    except Exception as e:
        logger.error(f"Debug endpoint error: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Quality Management CRUD endpoints
@app.route('/api/quality/bmw-claims', methods=['GET', 'POST'])
def manage_bmw_claims():
    """API endpoint for managing BMW/VW claims - GET all or POST new"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    if request.method == 'GET':
        try:
            claims = quality_db.get_bmw_quality_claims()
            return jsonify(claims)
        except Exception as e:
            logger.error(f"API error in get BMW claims: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'POST':
        try:
            claim_data = request.get_json()
            
            # Validate required fields
            required_fields = ['dateTime', 'event']
            for field in required_fields:
                if not claim_data.get(field):
                    return jsonify({'error': f'Missing required field: {field}'}), 400
            
            # Convert datetime string to proper format
            try:
                if 'T' in claim_data['dateTime']:
                    claim_time = datetime.fromisoformat(claim_data['dateTime'].replace('Z', '+00:00'))
                else:
                    claim_time = datetime.strptime(claim_data['dateTime'], '%Y-%m-%d %H:%M:%S')
                claim_data['dateTime'] = claim_time
            except (ValueError, KeyError) as ve:
                logger.error(f"Invalid datetime format: {str(ve)}")
                return jsonify({'error': 'Invalid datetime format'}), 400
            
            success = quality_db.add_bmw_claim(claim_data)
            
            return jsonify({
                'success': True, 
                'message': 'BMW claim added successfully'
            }), 201
            
        except Exception as e:
            logger.error(f"API error in add BMW claim: {str(e)}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/quality/bmw-claims/<claim_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_bmw_claim(claim_id):
    """API endpoint for managing a single BMW claim"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    if request.method == 'GET':
        try:
            claim = quality_db.get_bmw_claim_by_id(claim_id)
            if not claim:
                return jsonify({'error': 'Claim not found'}), 404
            return jsonify(claim)
        except Exception as e:
            logger.error(f"API error in get BMW claim: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            claim_data = request.get_json()
            
            # Convert datetime string to proper format
            if 'dateTime' in claim_data and claim_data['dateTime']:
                try:
                    if 'T' in claim_data['dateTime']:
                        claim_time = datetime.fromisoformat(claim_data['dateTime'].replace('Z', '+00:00'))
                    else:
                        claim_time = datetime.strptime(claim_data['dateTime'], '%Y-%m-%d %H:%M:%S')
                    claim_data['dateTime'] = claim_time
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format: {str(ve)}")
                    return jsonify({'error': 'Invalid datetime format'}), 400
            
            success = quality_db.update_bmw_claim(claim_id, claim_data)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'BMW claim updated successfully'
                })
            else:
                return jsonify({'error': 'Claim not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update BMW claim: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            success = quality_db.delete_bmw_claim(claim_id)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'BMW claim deleted successfully'
                })
            else:
                return jsonify({'error': 'Claim not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete BMW claim: {str(e)}")
            return jsonify({'error': str(e)}), 500
        
# Add these routes to your app.py file after the existing quality routes

# Additional Quality Management CRUD endpoints

@app.route('/api/quality/key-turns', methods=['POST'])
def add_key_turn():
    """Add a new key turn event"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        key_turn_data = request.get_json()
        
        # Validate required fields
        required_fields = ['dateTime', 'event']
        for field in required_fields:
            if not key_turn_data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Convert datetime string to proper format
        try:
            if 'T' in key_turn_data['dateTime']:
                key_turn_time = datetime.fromisoformat(key_turn_data['dateTime'].replace('Z', '+00:00'))
            else:
                key_turn_time = datetime.strptime(key_turn_data['dateTime'], '%Y-%m-%d %H:%M:%S')
            key_turn_data['dateTime'] = key_turn_time
        except (ValueError, KeyError) as ve:
            logger.error(f"Invalid datetime format: {str(ve)}")
            return jsonify({'error': 'Invalid datetime format'}), 400
        
        # Insert into database
        query = """
        INSERT INTO dbo.DailyKeyTurns (
            dateTime, event, description, orderNumber
        ) VALUES (?, ?, ?, ?)
        """
        
        params = [
            key_turn_data['dateTime'],
            key_turn_data.get('event', ''),
            key_turn_data.get('description', ''),
            key_turn_data.get('orderNumber', '')
        ]
        
        rows_affected = quality_db.execute_non_query(query, params)
        logger.info(f"Added key turn, rows affected: {rows_affected}")
        
        return jsonify({
            'success': True, 
            'message': 'Key turn added successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"API error in add key turn: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/key-turns/<key_turn_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_key_turn(key_turn_id):
    """API endpoint for managing a single key turn"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    if request.method == 'GET':
        try:
            query = "SELECT * FROM dbo.DailyKeyTurns WHERE keyTurnID = ?"
            df = quality_db.execute_query(query, params=[key_turn_id])
            
            if df.empty:
                return jsonify({'error': 'Key turn not found'}), 404
            
            key_turn = df.iloc[0].to_dict()
            
            # Convert to expected format
            for key, value in key_turn.items():
                if pd.isna(value):
                    key_turn[key] = None
                elif hasattr(value, 'isoformat'):
                    key_turn[key] = value.isoformat()
                else:
                    key_turn[key] = str(value) if value is not None else ''
            
            return jsonify(key_turn)
            
        except Exception as e:
            logger.error(f"Error getting key turn by ID: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            key_turn_data = request.get_json()
            
            # Convert datetime string to proper format
            if 'dateTime' in key_turn_data and key_turn_data['dateTime']:
                try:
                    if 'T' in key_turn_data['dateTime']:
                        key_turn_time = datetime.fromisoformat(key_turn_data['dateTime'].replace('Z', '+00:00'))
                    else:
                        key_turn_time = datetime.strptime(key_turn_data['dateTime'], '%Y-%m-%d %H:%M:%S')
                    key_turn_data['dateTime'] = key_turn_time
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format: {str(ve)}")
                    return jsonify({'error': 'Invalid datetime format'}), 400
            
            query = """
            UPDATE dbo.DailyKeyTurns SET 
                dateTime = ?, event = ?, description = ?, orderNumber = ?
            WHERE keyTurnID = ?
            """
            
            params = [
                key_turn_data.get('dateTime'),
                key_turn_data.get('event', ''),
                key_turn_data.get('description', ''),
                key_turn_data.get('orderNumber', ''),
                key_turn_id
            ]
            
            rows_affected = quality_db.execute_non_query(query, params)
            
            if rows_affected > 0:
                return jsonify({
                    'success': True,
                    'message': 'Key turn updated successfully'
                })
            else:
                return jsonify({'error': 'Key turn not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update key turn: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            query = "DELETE FROM dbo.DailyKeyTurns WHERE keyTurnID = ?"
            rows_affected = quality_db.execute_non_query(query, [key_turn_id])
            
            if rows_affected > 0:
                return jsonify({
                    'success': True,
                    'message': 'Key turn deleted successfully'
                })
            else:
                return jsonify({'error': 'Key turn not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete key turn: {str(e)}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/quality/line-quality', methods=['POST'])
def add_line_quality():
    """Add a new line quality issue"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        line_quality_data = request.get_json()
        
        # Validate required fields
        required_fields = ['dateTime', 'descriptionOfIssue']
        for field in required_fields:
            if not line_quality_data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Convert datetime string to proper format
        try:
            if 'T' in line_quality_data['dateTime']:
                line_quality_time = datetime.fromisoformat(line_quality_data['dateTime'].replace('Z', '+00:00'))
            else:
                line_quality_time = datetime.strptime(line_quality_data['dateTime'], '%Y-%m-%d %H:%M:%S')
            line_quality_data['dateTime'] = line_quality_time
        except (ValueError, KeyError) as ve:
            logger.error(f"Invalid datetime format: {str(ve)}")
            return jsonify({'error': 'Invalid datetime format'}), 400
        
        # Convert line to integer (it's an int column in your DB)
        line_number = line_quality_data.get('line', 1)
        try:
            line_number = int(line_number) if line_number else 1
        except (ValueError, TypeError):
            line_number = 1
        
        # Insert into database
        query = """
        INSERT INTO dbo.DailyLineQuality (
            dateTime, line, descriptionOfIssue, identifiedBy, actionTaken, owner, pdcaYN
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        params = [
            line_quality_data['dateTime'],
            line_number,  # Now properly converted to int
            str(line_quality_data.get('descriptionOfIssue', '')),
            str(line_quality_data.get('identifiedBy', '')),
            str(line_quality_data.get('actionTaken', '')),
            str(line_quality_data.get('owner', '')),
            str(line_quality_data.get('pdcaYN', ''))
        ]
        
        rows_affected = quality_db.execute_non_query(query, params)
        logger.info(f"Added line quality issue, rows affected: {rows_affected}")
        
        return jsonify({
            'success': True, 
            'message': 'Line quality issue added successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"API error in add line quality: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/line-quality/<line_quality_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_line_quality(line_quality_id):
    """API endpoint for managing a single line quality issue"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    if request.method == 'GET':
        try:
            query = "SELECT * FROM dbo.DailyLineQuality WHERE lineQualityID = ?"
            df = quality_db.execute_query(query, params=[line_quality_id])
            
            if df.empty:
                return jsonify({'error': 'Line quality issue not found'}), 404
            
            line_quality = df.iloc[0].to_dict()
            
            # Convert to expected format
            for key, value in line_quality.items():
                if pd.isna(value):
                    line_quality[key] = None
                elif hasattr(value, 'isoformat'):
                    line_quality[key] = value.isoformat()
                else:
                    line_quality[key] = str(value) if value is not None else ''
            
            return jsonify(line_quality)
            
        except Exception as e:
            logger.error(f"Error getting line quality by ID: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            line_quality_data = request.get_json()
            
            # Convert datetime string to proper format
            if 'dateTime' in line_quality_data and line_quality_data['dateTime']:
                try:
                    if 'T' in line_quality_data['dateTime']:
                        line_quality_time = datetime.fromisoformat(line_quality_data['dateTime'].replace('Z', '+00:00'))
                    else:
                        line_quality_time = datetime.strptime(line_quality_data['dateTime'], '%Y-%m-%d %H:%M:%S')
                    line_quality_data['dateTime'] = line_quality_time
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format: {str(ve)}")
                    return jsonify({'error': 'Invalid datetime format'}), 400
            
            query = """
            UPDATE dbo.DailyLineQuality SET 
                dateTime = ?, line = ?, descriptionOfIssue = ?, identifiedBy = ?, 
                actionTaken = ?, owner = ?, pdcaYN = ?
            WHERE lineQualityID = ?
            """
            
            params = [
                line_quality_data.get('dateTime'),
                line_quality_data.get('line', 1),
                line_quality_data.get('descriptionOfIssue', ''),
                line_quality_data.get('identifiedBy', ''),
                line_quality_data.get('actionTaken', ''),
                line_quality_data.get('owner', ''),
                line_quality_data.get('pdcaYN', ''),
                line_quality_id
            ]
            
            rows_affected = quality_db.execute_non_query(query, params)
            
            if rows_affected > 0:
                return jsonify({
                    'success': True,
                    'message': 'Line quality issue updated successfully'
                })
            else:
                return jsonify({'error': 'Line quality issue not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update line quality: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            query = "DELETE FROM dbo.DailyLineQuality WHERE lineQualityID = ?"
            rows_affected = quality_db.execute_non_query(query, [line_quality_id])
            
            if rows_affected > 0:
                return jsonify({
                    'success': True,
                    'message': 'Line quality issue deleted successfully'
                })
            else:
                return jsonify({'error': 'Line quality issue not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete line quality: {str(e)}")
            return jsonify({'error': str(e)}), 500




@app.route('/api/quality/supplier-containment', methods=['POST'])
def add_supplier_containment():
    """Add a new supplier containment action"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        containment_data = request.get_json()
        
        # Validate required fields
        required_fields = ['supplier', 'nonConformance']
        for field in required_fields:
            if not containment_data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Insert into database
        query = """
        INSERT INTO dbo.DailySupplierContainment (
            supplier, nonConformance, actionTaken, statusUpdate, owner, 
            totalPartsInspected, totalDefectParts, defectRate, endDateExpected
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        
        params = [
            containment_data.get('supplier', ''),
            containment_data.get('nonConformance', ''),
            containment_data.get('actionTaken', ''),
            containment_data.get('statusUpdate', ''),
            containment_data.get('owner', ''),
            containment_data.get('totalPartsInspected', 0),
            containment_data.get('totalDefectParts', 0),
            containment_data.get('defectRate', 0.0)
        ]
        
        rows_affected = quality_db.execute_non_query(query, params)
        logger.info(f"Added supplier containment, rows affected: {rows_affected}")
        
        return jsonify({
            'success': True, 
            'message': 'Supplier containment added successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"API error in add supplier containment: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/supplier-containment/<containment_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_supplier_containment(containment_id):
    """API endpoint for managing a single supplier containment action"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    if request.method == 'GET':
        try:
            query = "SELECT * FROM dbo.DailySupplierContainment WHERE containmentID = ?"
            df = quality_db.execute_query(query, params=[containment_id])
            
            if df.empty:
                return jsonify({'error': 'Supplier containment not found'}), 404
            
            containment = df.iloc[0].to_dict()
            
            # Convert to expected format
            for key, value in containment.items():
                if pd.isna(value):
                    containment[key] = None
                elif hasattr(value, 'isoformat'):
                    containment[key] = value.isoformat()
                else:
                    containment[key] = str(value) if value is not None else ''
            
            return jsonify(containment)
            
        except Exception as e:
            logger.error(f"Error getting supplier containment by ID: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            containment_data = request.get_json()
            
            query = """
            UPDATE dbo.DailySupplierContainment SET 
                supplier = ?, nonConformance = ?, actionTaken = ?, statusUpdate = ?, 
                owner = ?, totalPartsInspected = ?, totalDefectParts = ?, defectRate = ?
            WHERE containmentID = ?
            """
            
            params = [
                containment_data.get('supplier', ''),
                containment_data.get('nonConformance', ''),
                containment_data.get('actionTaken', ''),
                containment_data.get('statusUpdate', ''),
                containment_data.get('owner', ''),
                containment_data.get('totalPartsInspected', 0),
                containment_data.get('totalDefectParts', 0),
                containment_data.get('defectRate', 0.0),
                containment_id
            ]
            
            rows_affected = quality_db.execute_non_query(query, params)
            
            if rows_affected > 0:
                return jsonify({
                    'success': True,
                    'message': 'Supplier containment updated successfully'
                })
            else:
                return jsonify({'error': 'Supplier containment not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update supplier containment: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            query = "DELETE FROM dbo.DailySupplierContainment WHERE containmentID = ?"
            rows_affected = quality_db.execute_non_query(query, [containment_id])
            
            if rows_affected > 0:
                return jsonify({
                    'success': True,
                    'message': 'Supplier containment deleted successfully'
                })
            else:
                return jsonify({'error': 'Supplier containment not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete supplier containment: {str(e)}")
            return jsonify({'error': str(e)}), 500
        


# Add these new API routes to your app.py file

@app.route('/api/quality/eol-findings')
def quality_eol_findings():
    """API endpoint for EOL findings"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        hours = request.args.get('hours', 24, type=int)
        findings = quality_db.get_eol_findings(hours)
        return jsonify(findings)
    except Exception as e:
        logger.error(f"API error in quality_eol_findings: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/eol-findings/recent')
def quality_eol_findings_recent():
    """API endpoint for recent EOL findings (for expandable cards)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        findings = quality_db.get_eol_findings(24)  # Last 24 hours
        return jsonify(findings)
    except Exception as e:
        logger.error(f"API error in quality_eol_findings_recent: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/quality-alerts')
def quality_alerts_active():
    """API endpoint for active quality alerts from QualityAlert_Main"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        alerts = quality_db.get_active_quality_alerts()
        return jsonify(alerts)
    except Exception as e:
        logger.error(f"API error in quality_alerts_active: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality/quality-alerts/recent')
def quality_alerts_active_recent():
    """API endpoint for recent active quality alerts (for expandable cards)"""
    if not quality_db:
        return jsonify({'error': 'Quality database not available'}), 503
    
    try:
        alerts = quality_db.get_active_quality_alerts()
        return jsonify(alerts)
    except Exception as e:
        logger.error(f"API error in quality_alerts_active_recent: {str(e)}")
        return jsonify({'error': str(e)}), 500



class ProductionDatabase:
    def __init__(self):
        try:
            conn = self.get_connection()
            conn.close()
            logger.info("ProductionDatabase initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ProductionDatabase: {str(e)}")
            raise
    
    def get_connection(self):
        return get_db_connection_1()
    
    def execute_query(self, query, params=None):
        """Execute query with proper connection handling"""
        conn = None
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Production query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_non_query(self, query, params=None):
        """Execute non-query (INSERT, UPDATE, DELETE) with proper connection handling"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Production non-query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                
    
    def get_weld_downtime_summary(self):
        """Get weld downtime summary from WELD_Production database filtered by dateClosed"""
        try:
            conn = None
            try:
                conn = get_weld_db_connection()  # Connect to WELD_Production
                
                # Apply Monday exception logic
                today = datetime.now().weekday()  # Monday = 0, Sunday = 6
                if today == 0:  # Monday
                    hours_back = -72  # Last 72 hours on Monday
                else:
                    hours_back = -24  # Last 24 hours on other days
                
                query = """
                SELECT 
                    COUNT(*) as total_events,
                    ISNULL(SUM(minutes), 0) as total_minutes
                FROM [dbo].[Downtime]
                WHERE dateClosed IS NOT NULL
                AND dateClosed >= DATEADD(hour, ?, GETDATE())
                AND minutes IS NOT NULL  
                AND minutes > 0
                """
                df = pd.read_sql(query, conn, params=[hours_back])
                
                period_data = df.iloc[0] if not df.empty else {}
                
                def safe_int(value, default=0):
                    try:
                        return int(value) if pd.notna(value) and value is not None else default
                    except (ValueError, TypeError):
                        return default
                
                summary = {
                    'today_weld_total_events': safe_int(period_data.get('total_events', 0)),
                    'today_weld_total_minutes': safe_int(period_data.get('total_minutes', 0)),
                    'today_weld_maintenance_minutes': safe_int(period_data.get('total_minutes', 0))
                }
                
                logger.info(f"Weld downtime summary (last 24/72 hours): {summary}")
                return summary
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error generating weld downtime summary: {str(e)}")
            return {
                'today_weld_total_events': 0,
                'today_weld_total_minutes': 0,
                'today_weld_maintenance_minutes': 0
            }

    def get_weld_downtime_events(self, days=7, downtime_type='all'):
        """Get weld downtime events from WELD_Production database filtered by dateClosed"""
        try:
            conn = None
            try:
                conn = get_weld_db_connection()
                
                # Apply Monday exception logic for filtering
                today = datetime.now().weekday()  # Monday = 0, Sunday = 6
                if today == 0:  # Monday
                    hours_back = -72  # Last 72 hours on Monday
                else:
                    hours_back = -24  # Last 24 hours on other days
                
                query = """
                SELECT 
                    startTime,
                    endTime,
                    closed,
                    dateClosed,
                    reason,
                    minutes,
                    OperationID
                FROM [dbo].[Downtime]
                WHERE dateClosed IS NOT NULL
                AND dateClosed >= DATEADD(hour, ?, GETDATE())
                AND minutes IS NOT NULL
                AND minutes > 0
                ORDER BY minutes DESC  -- This orders by duration (highest first)

                """
                
                df = pd.read_sql(query, conn, params=[hours_back])
                
                events = []
                for _, row in df.iterrows():
                    # Convert datetime properly
                    start_time = row.get('startTime')
                    end_time = row.get('endTime')
                    date_closed = row.get('dateClosed')
                    
                    start_time_str = start_time.isoformat() if pd.notna(start_time) and hasattr(start_time, 'isoformat') else str(start_time) if pd.notna(start_time) else 'N/A'
                    end_time_str = end_time.isoformat() if pd.notna(end_time) and hasattr(end_time, 'isoformat') else str(end_time) if pd.notna(end_time) else 'N/A'
                    date_closed_str = date_closed.isoformat() if pd.notna(date_closed) and hasattr(date_closed, 'isoformat') else str(date_closed) if pd.notna(date_closed) else None
                    
                    # Determine status
                    closed = row.get('closed')
                    status = 'Closed' if pd.notna(closed) and str(closed).lower() == 'yes' else 'Open'
                    
                    # Get duration from minutes column
                    duration_minutes = int(row.get('minutes', 0)) if pd.notna(row.get('minutes')) else 0
                    
                    events.append({
                        'startTime': start_time_str,
                        'endTime': end_time_str,
                        'closed': str(closed) if pd.notna(closed) else 'No',
                        'dateClosed': date_closed_str,
                        'reason': str(row.get('reason', '') if pd.notna(row.get('reason')) else ''),
                        'fourM': 'MACHINE',  # All weld downtime is machine-related
                        'whoFixed': '',  # Not in your table
                        'durationMinutes': duration_minutes,
                        'eventType': 'Maintenance',
                        'status': status,
                        'operationID': str(row.get('OperationID', '') if pd.notna(row.get('OperationID')) else '')
                    })
                
                return events
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting weld downtime events: {str(e)}")
            return []

    

    def get_jph_pplhdit_data(self, limit=4):
        """Get averaged JPH and PPLHDIT data from the last N different mainIDs where status is closed"""
        conn = None
        try:
            conn = get_db_connection()  # Production database
            
            # Updated query to return exact hours as decimal
            query = """
            SELECT 
                j.mainID,
                m.costCenterID,
                AVG(CAST(j.JPH AS FLOAT)) as avg_JPH,
                AVG(CAST(j.PPLHDIT AS FLOAT)) as avg_PPLHDIT,
                COUNT(*) as hour_count,
                -- Calculate exact hours as decimal
                CAST(COUNT(*) AS FLOAT) as exact_hours,
                SUM(j.quantity) as total_quantity,
                SUM(j.runtime) as total_runtime,
                MAX(j.dateClosed) as latest_date,
                COALESCE(u.name, 'Unknown') as supervisor_name,
                CASE 
                    WHEN m.costCenterID = 1 THEN 'Front'
                    WHEN m.costCenterID = 2 THEN 'Rear' 
                    ELSE 'Unknown'
                END as costCenter,
                CASE 
                    WHEN m.costCenterID = 1 THEN 1.65  -- Front target
                    WHEN m.costCenterID = 2 THEN 1.21  -- Rear target
                    ELSE 1.50
                END as target_PPLHDIT
            FROM (
                SELECT DISTINCT TOP (?) mainID
                FROM [G0X_Production].[dbo].[Jobs]
                WHERE JPH IS NOT NULL 
                AND PPLHDIT IS NOT NULL 
                AND Closed = 'Yes'
                ORDER BY mainID DESC
            ) top_mains
            INNER JOIN [G0X_Production].[dbo].[Jobs] j ON j.mainID = top_mains.mainID
            INNER JOIN [G0X_Production].[dbo].[Main] m ON j.mainID = m.mainID
            LEFT JOIN [G0X_Production].[dbo].[Users] u ON m.userID = u.userID
            WHERE j.JPH IS NOT NULL 
            AND j.PPLHDIT IS NOT NULL 
            AND j.Closed = 'Yes'
            GROUP BY j.mainID, m.costCenterID, u.name
            ORDER BY j.mainID DESC
            """
            df = pd.read_sql(query, conn, params=[limit])
            
            reports = []
            for _, row in df.iterrows():
                # Use exact_hours instead of hour_count for display
                exact_hours = float(row.get('exact_hours', 0)) if pd.notna(row.get('exact_hours')) else 0
                
                report_data = {
                    'mainID': str(row.get('mainID', '')),
                    'costCenterID': int(row.get('costCenterID', 0)) if pd.notna(row.get('costCenterID')) else 0,
                    'costCenter': str(row.get('costCenter', 'Unknown')),
                    'target_PPLHDIT': float(row.get('target_PPLHDIT', 1.50)),
                    'avg_JPH': float(row.get('avg_JPH', 0)) if pd.notna(row.get('avg_JPH')) else 0,
                    'avg_PPLHDIT': float(row.get('avg_PPLHDIT', 0)) if pd.notna(row.get('avg_PPLHDIT')) else 0,
                    'hour_count': int(row.get('hour_count', 0)) if pd.notna(row.get('hour_count')) else 0,
                    'exact_hours': exact_hours,  # Add this field
                    'total_quantity': int(row.get('total_quantity', 0)) if pd.notna(row.get('total_quantity')) else 0,
                    'total_runtime': int(row.get('total_runtime', 0)) if pd.notna(row.get('total_runtime')) else 0,
                    'latest_date': str(row.get('latest_date', '')) if pd.notna(row.get('latest_date')) else '',
                    'supervisor_name': str(row.get('supervisor_name', 'Unknown')) if pd.notna(row.get('supervisor_name')) else 'Unknown'
                }
                reports.append(report_data)
            
            return reports
            
        except Exception as e:
            logger.error(f"Error getting JPH/PPLHDIT data: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    
    
    def get_downtime_summary(self):
        """Get downtime summary statistics from G0X_Production database"""
        try:
            # Use Monday-aware time filtering
            today = datetime.now().weekday()  # Monday = 0, Sunday = 6
            if today == 0:  # Monday
                hours_back = -72  # Last 72 hours on Monday
                period_label = "last 3 days"
            else:
                hours_back = -24  # Last 24 hours on other days
                period_label = "today"
            
            conn = None
            try:
                conn = get_db_connection()  # This connects to G0X_Production
                
                # Get downtime data for the appropriate period

                period_query = f"""
                SELECT 
                    COUNT(*) as total_events,
                    SUM(CASE WHEN d.fourM IN ('MAN', 'METHOD') THEN 1 ELSE 0 END) as production_events,
                    SUM(CASE WHEN d.fourM = 'MACHINE' THEN 1 ELSE 0 END) as maintenance_events,
                    SUM(DATEDIFF(MINUTE, d.startTime, d.endTime)) as total_minutes,
                    SUM(CASE WHEN d.fourM IN ('MAN', 'METHOD') THEN DATEDIFF(MINUTE, d.startTime, d.endTime) ELSE 0 END) as production_minutes,
                    SUM(CASE WHEN d.fourM = 'MACHINE' THEN DATEDIFF(MINUTE, d.startTime, d.endTime) ELSE 0 END) as maintenance_minutes
                FROM [dbo].[Downtime] d
                INNER JOIN [dbo].[main] main ON d.mainID = main.mainID
                WHERE d.startTime >= DATEADD(hour, ?, GETDATE())
                AND d.endTime IS NOT NULL
                AND main.closed = 'Yes'
                """
                period_df = pd.read_sql(period_query, conn, params=[hours_back])

                
                # Get week's downtime data (always 7 days)
                
                week_query = """
                SELECT 
                    COUNT(*) as total_events,
                    SUM(CASE WHEN d.fourM IN ('MAN', 'METHOD') THEN 1 ELSE 0 END) as production_events,
                    SUM(CASE WHEN d.fourM = 'MACHINE' THEN 1 ELSE 0 END) as maintenance_events,
                    SUM(DATEDIFF(MINUTE, d.startTime, d.endTime)) as total_minutes,
                    SUM(CASE WHEN d.fourM IN ('MAN', 'METHOD') THEN DATEDIFF(MINUTE, d.startTime, d.endTime) ELSE 0 END) as production_minutes,
                    SUM(CASE WHEN d.fourM = 'MACHINE' THEN DATEDIFF(MINUTE, d.startTime, d.endTime) ELSE 0 END) as maintenance_minutes
                FROM [dbo].[Downtime] d
                INNER JOIN [dbo].[main] main ON d.mainID = main.mainID
                WHERE d.startTime >= DATEADD(day, -7, GETDATE())
                AND d.endTime IS NOT NULL
                AND main.closed = 'Yes'
                """
                week_df = pd.read_sql(week_query, conn)

                
                # Process results
                period_data = period_df.iloc[0] if not period_df.empty else {}
                week_data = week_df.iloc[0] if not week_df.empty else {}
                
                def safe_int(value, default=0):
                    try:
                        return int(value) if pd.notna(value) and value is not None else default
                    except (ValueError, TypeError):
                        return default
                
                summary = {
                    f'{period_label}_total_events': safe_int(period_data.get('total_events', 0)),
                    f'{period_label}_production_events': safe_int(period_data.get('production_events', 0)),
                    f'{period_label}_maintenance_events': safe_int(period_data.get('maintenance_events', 0)),
                    f'{period_label}_total_minutes': safe_int(period_data.get('total_minutes', 0)),
                    f'{period_label}_production_minutes': safe_int(period_data.get('production_minutes', 0)),
                    f'{period_label}_maintenance_minutes': safe_int(period_data.get('maintenance_minutes', 0)),
                    'week_total_events': safe_int(week_data.get('total_events', 0)),
                    'week_production_events': safe_int(week_data.get('production_events', 0)),
                    'week_maintenance_events': safe_int(week_data.get('maintenance_events', 0)),
                    'week_total_minutes': safe_int(week_data.get('total_minutes', 0)),
                    'week_production_minutes': safe_int(week_data.get('production_minutes', 0)),
                    'week_maintenance_minutes': safe_int(week_data.get('maintenance_minutes', 0))
                }
                
                # Add compatibility keys for existing code
                summary['today_total_events'] = summary[f'{period_label}_total_events']
                summary['today_production_events'] = summary[f'{period_label}_production_events']
                summary['today_maintenance_events'] = summary[f'{period_label}_maintenance_events']
                summary['today_total_minutes'] = summary[f'{period_label}_total_minutes']
                summary['today_production_minutes'] = summary[f'{period_label}_production_minutes']
                summary['today_maintenance_minutes'] = summary[f'{period_label}_maintenance_minutes']
                
                logger.info(f"Downtime summary generated for {period_label}: {summary}")
                return summary
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error generating downtime summary: {str(e)}")
            # Return default values on error
            return {
                'today_total_events': 0,
                'today_production_events': 0,
                'today_maintenance_events': 0,
                'today_total_minutes': 0,
                'today_production_minutes': 0,
                'today_maintenance_minutes': 0,
                'week_total_events': 0,
                'week_production_events': 0,
                'week_maintenance_events': 0,
                'week_total_minutes': 0,
                'week_production_minutes': 0,
                'week_maintenance_minutes': 0
            }


    
    
    def get_downtime_events(self, days=7, downtime_type='all'):
        """Get downtime events filtered by type from G0X_Production database"""
        try:
            # Handle Monday exception and default days
            if days is None:
                today = datetime.now().weekday()  # Monday = 0, Sunday = 6
                if today == 0:  # Monday
                    days = 3  # Use 3 days on Monday
                else:
                    days = 1  # Use 1 day on other days
            
            # Build WHERE clause based on downtime_type
            if downtime_type == 'production':
                type_filter = "AND fourM IN ('MAN', 'METHOD')"
            elif downtime_type == 'maintenance':
                type_filter = "AND fourM = 'MACHINE'"
            else:
                type_filter = ""
            
            hours_back = -24 * days  # Convert days to hours
            
            # Use production database connection
            conn = None
            try:
                conn = get_db_connection()  # This connects to G0X_Production
                


                query = f"""
                SELECT 
                    d.startTime,
                    d.endTime,
                    d.reason,
                    d.fourM,
                    d.whoFixed,
                    CONCAT(ISNULL(mach.line, ''), '-', ISNULL(mach.machine, '')) AS machineLocation,
                    CASE 
                        WHEN d.endTime IS NOT NULL THEN DATEDIFF(MINUTE, d.startTime, d.endTime)
                        ELSE DATEDIFF(MINUTE, d.startTime, GETDATE())
                    END as duration_minutes
                FROM [dbo].[Downtime] d
                LEFT JOIN [dbo].[Machine] mach ON d.machineID = mach.machineID
                INNER JOIN [dbo].[main] main ON d.mainID = main.mainID
                WHERE d.startTime >= DATEADD(hour, ?, GETDATE())
                {type_filter}
                AND d.endTime IS NOT NULL
                AND main.closed = 'Yes'
                ORDER BY duration_minutes DESC
                """


                
                df = pd.read_sql(query, conn, params=[hours_back])
                
                events = []
                for _, row in df.iterrows():
                    # Convert datetime properly
                    start_time = row.get('startTime')
                    end_time = row.get('endTime')
                    date_closed = row.get('dateClosed')
                    
                    if pd.notna(start_time):
                        if hasattr(start_time, 'isoformat'):
                            start_time_str = start_time.isoformat()
                        else:
                            start_time_str = str(start_time)
                    else:
                        start_time_str = 'N/A'
                    
                    if pd.notna(end_time):
                        if hasattr(end_time, 'isoformat'):
                            end_time_str = end_time.isoformat()
                        else:
                            end_time_str = str(end_time)
                    else:
                        end_time_str = 'N/A'
                    
                    if pd.notna(date_closed):
                        if hasattr(date_closed, 'isoformat'):
                            date_closed_str = date_closed.isoformat()
                        else:
                            date_closed_str = str(date_closed)
                    else:
                        date_closed_str = None
                    
                    # Determine event type and status
                    four_m = str(row.get('fourM', ''))
                    if four_m in ['MAN', 'METHOD']:
                        event_type = 'Production'
                    elif four_m == 'MACHINE':
                        event_type = 'Maintenance'
                    else:
                        event_type = 'Other'
                    
                    # Determine status
                    closed = row.get('closed')
                    if pd.notna(closed) and str(closed).lower() == 'yes':
                        status = 'Closed'
                    else:
                        status = 'Open'
                    
                    # Calculate duration
                    duration = row.get('duration_minutes')
                    if pd.notna(duration):
                        duration_minutes = int(duration)
                    else:
                        duration_minutes = 0
                    
                    events.append({
                        'startTime': start_time_str,
                        'endTime': end_time_str,
                        'closed': str(closed) if pd.notna(closed) else 'No',
                        'dateClosed': date_closed_str,
                        'reason': str(row.get('reason', '') if pd.notna(row.get('reason')) else ''),
                        'fourM': four_m,
                        'whoFixed': str(row.get('whoFixed', '') if pd.notna(row.get('whoFixed')) else ''),
                        'machineLocation': str(row.get('machineLocation', '') if pd.notna(row.get('machineLocation')) else 'N/A'),
                        'durationMinutes': duration_minutes,
                        'eventType': event_type,
                        'status': status
                    })
                
                return events
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting downtime events: {str(e)}")
            return []

    def get_daily_production_summary(self):
        """Get daily production summary statistics including labor data"""
        try:
            # First check what columns actually exist
            try:
                structure = self.get_table_structure()
                logger.info(f"Table columns: {[col['COLUMN_NAME'] for col in structure]}")
            except:
                pass  # Continue even if we can't get structure
            
            # Get today's data
            today_query = """
            SELECT * FROM dbo.DailyProduction 
            WHERE ProductionDate >= DATEADD(hour, -24, GETDATE())
            """
            today_df = self.execute_query(today_query)
            
            # Get latest record if no today's data
            latest_query = """
            SELECT TOP 1 * FROM dbo.DailyProduction 
            ORDER BY ProductionDate DESC
            """
            latest_df = self.execute_query(latest_query)
            
            # Use today's data if available, otherwise latest
            data_df = today_df if not today_df.empty else latest_df
            
            if data_df.empty:
                return {
                    'total_headcount': 0,
                    'new_hires_today': 0,
                    'total_terms_today': 0,
                    'benteler_headcount': 0,
                    'benteler_new_hires': 0,
                    'benteler_terms': 0,
                    'prologistix_headcount': 0,
                    'prologistix_new_hires': 0,
                    'prologistix_terms': 0,
                    'production_date': str(date.today()),
                    'bmw_day': {
                        'total_personnel': 0,
                        'callouts': 0,
                        'vacations': 0,
                        'overtime_headcount': 0,
                        'front_net_hours': 0,
                        'rear_net_hours': 0,
                        'front_ot_hours': 0,
                        'rear_ot_hours': 0
                    },
                    'bmw_night': {
                        'total_personnel': 0,
                        'callouts': 0,
                        'vacations': 0,
                        'overtime_headcount': 0,
                        'front_net_hours': 0,
                        'rear_net_hours': 0,
                        'front_ot_hours': 0,
                        'rear_ot_hours': 0
                    },
                    'weld_day': {
                        'total_personnel': 0,
                        'callouts': 0,
                        'vacations': 0,
                        'net_hours': 0,
                        'ot_hours': 0
                    },
                    'weld_night': {
                        'total_personnel': 0,
                        'callouts': 0,
                        'vacations': 0,
                        'net_hours': 0,
                        'ot_hours': 0
                    },
                    'total_labor_hours': 0,
                    'today_downtime_minutes': 0,
                    'today_production_downtime': 0,
                    'today_maintenance_downtime': 0,
                    'today_downtime_events': 0,
                    'today_production_events': 0,
                    'today_maintenance_events': 0,
                    'today_weld_downtime_minutes': 0,
                    'today_weld_downtime_events': 0
                }
            
            row = data_df.iloc[0]
            
            # Helper function to safely get float values
            def safe_float(value, default=0.0):
                try:
                    return float(value) if pd.notna(value) else default
                except (ValueError, TypeError):
                    return default
            
            # Helper function to safely get int values
            def safe_int(value, default=0):
                try:
                    return int(value) if pd.notna(value) else default
                except (ValueError, TypeError):
                    return default
            
            # Calculate total labor hours
            bmw_day_net = safe_float(row.get('BMWDayFrontNetHours', 0)) + safe_float(row.get('BMWDayRearNetHours', 0))
            bmw_day_ot = safe_float(row.get('BMWDayFrontOTHours', 0)) + safe_float(row.get('BMWDayRearOTHours', 0))
            bmw_night_net = safe_float(row.get('BMWNightFrontNetHours', 0)) + safe_float(row.get('BMWNightRearNetHours', 0))
            bmw_night_ot = safe_float(row.get('BMWNightFrontOTHours', 0)) + safe_float(row.get('BMWNightRearOTHours', 0))
            weld_day_net = safe_float(row.get('WeldDayNetHours', 0))
            weld_day_ot = safe_float(row.get('WeldDayOTHours', 0))
            weld_night_net = safe_float(row.get('WeldNightNetHours', 0))
            weld_night_ot = safe_float(row.get('WeldNightOTHours', 0))
            
            total_labor_hours = bmw_day_net + bmw_day_ot + bmw_night_net + bmw_night_ot + weld_day_net + weld_day_ot + weld_night_net + weld_night_ot
            
            # Get regular downtime summary
            downtime_summary = self.get_downtime_summary()
            
            # Try to get weld downtime summary, but don't fail if it's unavailable
            weld_downtime_summary = {
                'today_weld_total_events': 0,
                'today_weld_total_minutes': 0,
                'today_weld_maintenance_minutes': 0
            }
            
            try:
                logger.info("Attempting to get weld downtime summary...")
                weld_downtime_summary = self.get_weld_downtime_summary()
                logger.info(f"Weld downtime summary retrieved: {weld_downtime_summary}")
            except Exception as weld_error:
                logger.warning(f"Weld downtime unavailable, using defaults: {str(weld_error)}")
                # Keep default values defined above
            
            summary = {
                'total_headcount': safe_int(row.get('TotalHeadcount', 0)),
                'new_hires_today': safe_int(row.get('NewHires', 0)),
                'total_terms_today': safe_int(row.get('TotalTerms', 0)),
                'benteler_headcount': safe_int(row.get('BHeadcount', 0)),
                'benteler_new_hires': safe_int(row.get('BNewHires', 0)),
                'benteler_terms': safe_int(row.get('BTerms', 0)),
                'prologistix_headcount': safe_int(row.get('PHeadcount', 0)),
                'prologistix_new_hires': safe_int(row.get('PNewHires', 0)),
                'prologistix_terms': safe_int(row.get('PTerms', 0)),
                'production_date': str(row.get('ProductionDate', date.today())),
                
                # BMW Day Shift Labor Data
                'bmw_day': {
                    'total_personnel': safe_int(row.get('BMWDayTotalPersonnel', 0)),
                    'callouts': safe_int(row.get('BMWDayCallouts', 0)),
                    'vacations': safe_int(row.get('BMWDayVacations', 0)),
                    'overtime_headcount': safe_int(row.get('BMWDayOvertimeHeadcount', 0)),
                    'front_net_hours': safe_float(row.get('BMWDayFrontNetHours', 0)),
                    'rear_net_hours': safe_float(row.get('BMWDayRearNetHours', 0)),
                    'front_ot_hours': safe_float(row.get('BMWDayFrontOTHours', 0)),
                    'rear_ot_hours': safe_float(row.get('BMWDayRearOTHours', 0))
                },
                
                # BMW Night Shift Labor Data
                'bmw_night': {
                    'total_personnel': safe_int(row.get('BMWNightTotalPersonnel', 0)),
                    'callouts': safe_int(row.get('BMWNightCallouts', 0)),
                    'vacations': safe_int(row.get('BMWNightVacations', 0)),
                    'overtime_headcount': safe_int(row.get('BMWNightOvertimeHeadcount', 0)),
                    'front_net_hours': safe_float(row.get('BMWNightFrontNetHours', 0)),
                    'rear_net_hours': safe_float(row.get('BMWNightRearNetHours', 0)),
                    'front_ot_hours': safe_float(row.get('BMWNightFrontOTHours', 0)),
                    'rear_ot_hours': safe_float(row.get('BMWNightRearOTHours', 0))
                },
                
                # Weld Day Shift Labor Data
                'weld_day': {
                    'total_personnel': safe_int(row.get('WeldDayTotalPersonnel', 0)),
                    'callouts': safe_int(row.get('WeldDayCallouts', 0)),
                    'vacations': safe_int(row.get('WeldDayVacations', 0)),
                    'net_hours': safe_float(row.get('WeldDayNetHours', 0)),
                    'ot_hours': safe_float(row.get('WeldDayOTHours', 0))
                },
                
                # Weld Night Shift Labor Data
                'weld_night': {
                    'total_personnel': safe_int(row.get('WeldNightTotalPersonnel', 0)),
                    'callouts': safe_int(row.get('WeldNightCallouts', 0)),
                    'vacations': safe_int(row.get('WeldNightVacations', 0)),
                    'net_hours': safe_float(row.get('WeldNightNetHours', 0)),
                    'ot_hours': safe_float(row.get('WeldNightOTHours', 0))
                },
                
                'total_labor_hours': round(total_labor_hours, 2),
                
                # Regular downtime data
                'today_downtime_minutes': downtime_summary.get('today_total_minutes', 0),
                'today_production_downtime': downtime_summary.get('today_production_minutes', 0),
                'today_maintenance_downtime': downtime_summary.get('today_maintenance_minutes', 0),
                'today_downtime_events': downtime_summary.get('today_total_events', 0),
                'today_production_events': downtime_summary.get('today_production_events', 0),
                'today_maintenance_events': downtime_summary.get('today_maintenance_events', 0),
                
                # Weld downtime data (with graceful fallback)
                'today_weld_downtime_minutes': weld_downtime_summary.get('today_weld_total_minutes', 0),
                'today_weld_downtime_events': weld_downtime_summary.get('today_weld_total_events', 0)
            }
            
            logger.info(f"Production summary completed successfully with weld data: {summary.get('today_weld_downtime_minutes', 0)} weld minutes")
            return summary
            
        except Exception as e:
            logger.error(f"Error getting daily production summary with labor: {str(e)}", exc_info=True)
            raise

    def upsert_daily_production(self, production_data):
        """Update existing record for the date or insert new one (UPSERT logic)"""
        try:
            # Convert date string to proper format
            production_date = production_data.get('productionDate')
            if production_date:
                try:
                    if 'T' in production_date:
                        date_obj = datetime.fromisoformat(production_date.replace('Z', '+00:00')).date()
                    else:
                        date_obj = datetime.strptime(production_date, '%Y-%m-%d').date()
                except (ValueError, KeyError):
                    date_obj = date.today()
            else:
                date_obj = date.today()
            
            # Get user from session or default
            user = production_data.get('createdBy', 'System')
            
            # Check if record exists for this date - using only ProductionDate column
            check_query = """
            SELECT TOP 1 * FROM dbo.DailyProduction 
            WHERE ProductionDate = ?
            ORDER BY ProductionDate DESC
            """
            existing_df = self.execute_query(check_query, params=[date_obj])
            
            if not existing_df.empty:
                # UPDATE existing record - use ProductionDate as identifier since ProductionID doesn't exist
                logger.info(f"Updating existing production record for date {date_obj}")
                
                update_query = """
                UPDATE dbo.DailyProduction SET 
                    NewHires = ?, TotalTerms = ?, TotalHeadcount = ?,
                    BNewHires = ?, BTerms = ?, BHeadcount = ?, 
                    PNewHires = ?, PTerms = ?, PHeadcount = ?,
                    BMWDayTotalPersonnel = ?, BMWDayCallouts = ?, BMWDayVacations = ?, 
                    BMWDayOvertimeHeadcount = ?, BMWDayFrontNetHours = ?, BMWDayRearNetHours = ?, 
                    BMWDayFrontOTHours = ?, BMWDayRearOTHours = ?,
                    BMWNightTotalPersonnel = ?, BMWNightCallouts = ?, BMWNightVacations = ?, 
                    BMWNightOvertimeHeadcount = ?, BMWNightFrontNetHours = ?, BMWNightRearNetHours = ?, 
                    BMWNightFrontOTHours = ?, BMWNightRearOTHours = ?,
                    WeldDayTotalPersonnel = ?, WeldDayCallouts = ?, WeldDayVacations = ?, 
                    WeldDayNetHours = ?, WeldDayOTHours = ?,
                    WeldNightTotalPersonnel = ?, WeldNightCallouts = ?, WeldNightVacations = ?, 
                    WeldNightNetHours = ?, WeldNightOTHours = ?
                WHERE ProductionDate = ?
                """
                
                params = [
                    # Headcount data
                    int(production_data.get('newHires', 0)),
                    int(production_data.get('totalTerms', 0)),
                    int(production_data.get('totalHeadcount', 0)),
                    int(production_data.get('bNewHires', 0)),
                    int(production_data.get('bTerms', 0)),
                    int(production_data.get('bHeadcount', 0)),
                    int(production_data.get('pNewHires', 0)),
                    int(production_data.get('pTerms', 0)),
                    int(production_data.get('pHeadcount', 0)),
                    # BMW Day Labor data
                    int(production_data.get('bmwDayTotalPersonnel', 0)),
                    int(production_data.get('bmwDayCallouts', 0)),
                    int(production_data.get('bmwDayVacations', 0)),
                    int(production_data.get('bmwDayOvertimeHeadcount', 0)),
                    float(production_data.get('bmwDayFrontNetHours', 0)),
                    float(production_data.get('bmwDayRearNetHours', 0)),
                    float(production_data.get('bmwDayFrontOTHours', 0)),
                    float(production_data.get('bmwDayRearOTHours', 0)),
                    # BMW Night Labor data
                    int(production_data.get('bmwNightTotalPersonnel', 0)),
                    int(production_data.get('bmwNightCallouts', 0)),
                    int(production_data.get('bmwNightVacations', 0)),
                    int(production_data.get('bmwNightOvertimeHeadcount', 0)),
                    float(production_data.get('bmwNightFrontNetHours', 0)),
                    float(production_data.get('bmwNightRearNetHours', 0)),
                    float(production_data.get('bmwNightFrontOTHours', 0)),
                    float(production_data.get('bmwNightRearOTHours', 0)),
                    # Weld Day Labor data
                    int(production_data.get('weldDayTotalPersonnel', 0)),
                    int(production_data.get('weldDayCallouts', 0)),
                    int(production_data.get('weldDayVacations', 0)),
                    float(production_data.get('weldDayNetHours', 0)),
                    float(production_data.get('weldDayOTHours', 0)),
                    # Weld Night Labor data
                    int(production_data.get('weldNightTotalPersonnel', 0)),
                    int(production_data.get('weldNightCallouts', 0)),
                    int(production_data.get('weldNightVacations', 0)),
                    float(production_data.get('weldNightNetHours', 0)),
                    float(production_data.get('weldNightOTHours', 0)),
                    # WHERE clause
                    date_obj
                ]
                
                rows_affected = self.execute_non_query(update_query, params)
                logger.info(f"Updated daily production record for {date_obj}, rows affected: {rows_affected}")
                return True
                
            else:
                # INSERT new record
                logger.info(f"Creating new production record for date {date_obj}")
                
                insert_query = """
                INSERT INTO dbo.DailyProduction (
                    ProductionDate, NewHires, TotalTerms, TotalHeadcount,
                    BNewHires, BTerms, BHeadcount, PNewHires, PTerms, PHeadcount,
                    BMWDayTotalPersonnel, BMWDayCallouts, BMWDayVacations, 
                    BMWDayOvertimeHeadcount, BMWDayFrontNetHours, BMWDayRearNetHours, 
                    BMWDayFrontOTHours, BMWDayRearOTHours,
                    BMWNightTotalPersonnel, BMWNightCallouts, BMWNightVacations, 
                    BMWNightOvertimeHeadcount, BMWNightFrontNetHours, BMWNightRearNetHours, 
                    BMWNightFrontOTHours, BMWNightRearOTHours,
                    WeldDayTotalPersonnel, WeldDayCallouts, WeldDayVacations, 
                    WeldDayNetHours, WeldDayOTHours,
                    WeldNightTotalPersonnel, WeldNightCallouts, WeldNightVacations, 
                    WeldNightNetHours, WeldNightOTHours
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                params = [
                    date_obj,
                    # Headcount data
                    int(production_data.get('newHires', 0)),
                    int(production_data.get('totalTerms', 0)),
                    int(production_data.get('totalHeadcount', 0)),
                    int(production_data.get('bNewHires', 0)),
                    int(production_data.get('bTerms', 0)),
                    int(production_data.get('bHeadcount', 0)),
                    int(production_data.get('pNewHires', 0)),
                    int(production_data.get('pTerms', 0)),
                    int(production_data.get('pHeadcount', 0)),
                    # BMW Day Labor data
                    int(production_data.get('bmwDayTotalPersonnel', 0)),
                    int(production_data.get('bmwDayCallouts', 0)),
                    int(production_data.get('bmwDayVacations', 0)),
                    int(production_data.get('bmwDayOvertimeHeadcount', 0)),
                    float(production_data.get('bmwDayFrontNetHours', 0)),
                    float(production_data.get('bmwDayRearNetHours', 0)),
                    float(production_data.get('bmwDayFrontOTHours', 0)),
                    float(production_data.get('bmwDayRearOTHours', 0)),
                    # BMW Night Labor data
                    int(production_data.get('bmwNightTotalPersonnel', 0)),
                    int(production_data.get('bmwNightCallouts', 0)),
                    int(production_data.get('bmwNightVacations', 0)),
                    int(production_data.get('bmwNightOvertimeHeadcount', 0)),
                    float(production_data.get('bmwNightFrontNetHours', 0)),
                    float(production_data.get('bmwNightRearNetHours', 0)),
                    float(production_data.get('bmwNightFrontOTHours', 0)),
                    float(production_data.get('bmwNightRearOTHours', 0)),
                    # Weld Day Labor data
                    int(production_data.get('weldDayTotalPersonnel', 0)),
                    int(production_data.get('weldDayCallouts', 0)),
                    int(production_data.get('weldDayVacations', 0)),
                    float(production_data.get('weldDayNetHours', 0)),
                    float(production_data.get('weldDayOTHours', 0)),
                    # Weld Night Labor data
                    int(production_data.get('weldNightTotalPersonnel', 0)),
                    int(production_data.get('weldNightCallouts', 0)),
                    int(production_data.get('weldNightVacations', 0)),
                    float(production_data.get('weldNightNetHours', 0)),
                    float(production_data.get('weldNightOTHours', 0))
                ]
                
                rows_affected = self.execute_non_query(insert_query, params)
                logger.info(f"Inserted new daily production record for {date_obj}, rows affected: {rows_affected}")
                return True
            
        except Exception as e:
            logger.error(f"Error in upsert daily production record: {str(e)}")
            raise


    # Keep the original method for backward compatibility
    def add_daily_production(self, production_data):
        """Add a new daily production record - now redirects to upsert"""
        return self.upsert_daily_production(production_data)



@app.route('/api/production/downtime/weld-summary')
def weld_downtime_summary():
    """API endpoint for weld downtime summary"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        summary = production_db.get_weld_downtime_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in weld_downtime_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/photos/upload/maintenance', methods=['POST'])
def upload_maintenance_photo():
    """Upload a maintenance performance photo"""
    try:
        if 'photo' not in request.files:
            return jsonify({'error': 'No photo file provided'}), 400
        
        file = request.files['photo']
        photo_number = request.form.get('photoNumber', '1')
        maintenance_date = request.form.get('maintenanceDate', date.today().isoformat())
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if file and allowed_file(file.filename):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_extension = file.filename.rsplit('.', 1)[1].lower()
            
            # FIXED: Use consistent naming pattern
            filename = f"maintenance_{maintenance_date}_photo{photo_number}_{timestamp}.{file_extension}"
            filename = secure_filename(filename)
            
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            logger.info(f"Maintenance photo {photo_number} for {maintenance_date} uploaded: {filename}")
            return jsonify({
                'success': True,
                'filename': filename,
                'url': f'/photos/{filename}',
                'photoUrl': f'/photos/{filename}',
                'photoNumber': photo_number,
                'maintenanceDate': maintenance_date,
                'message': 'Photo uploaded successfully'
            })
        else:
            return jsonify({'error': 'Invalid file type'}), 400
            
    except Exception as e:
        logger.error(f"Error uploading maintenance photo: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/photos/maintenance')
def get_maintenance_photos():
    """Get maintenance performance photos for today"""
    try:
        today = date.today().isoformat()
        photos = {'photo1': None, 'photo2': None}
        
        if os.path.exists(app.config['UPLOAD_FOLDER']):
            all_files = os.listdir(app.config['UPLOAD_FOLDER'])
            
            # FIXED: Match the actual filename pattern
            # Look for today's photos first, then fall back to most recent
            photo1_files = sorted([
                f for f in all_files 
                if f.startswith(f'maintenance_{today}_photo1_')
            ], reverse=True)
            
            photo2_files = sorted([
                f for f in all_files 
                if f.startswith(f'maintenance_{today}_photo2_')
            ], reverse=True)
            
            # If no photos for today, get the most recent ones
            if not photo1_files:
                photo1_files = sorted([
                    f for f in all_files 
                    if 'photo1_' in f and f.startswith('maintenance_')
                ], reverse=True)
            
            if not photo2_files:
                photo2_files = sorted([
                    f for f in all_files 
                    if 'photo2_' in f and f.startswith('maintenance_')
                ], reverse=True)
            
            if photo1_files:
                photos['photo1'] = {
                    'filename': photo1_files[0],
                    'url': f'/photos/{photo1_files[0]}',
                    'photoUrl': f'/photos/{photo1_files[0]}'
                }
                logger.info(f"Found photo1: {photo1_files[0]}")
            
            if photo2_files:
                photos['photo2'] = {
                    'filename': photo2_files[0],
                    'url': f'/photos/{photo2_files[0]}',
                    'photoUrl': f'/photos/{photo2_files[0]}'
                }
                logger.info(f"Found photo2: {photo2_files[0]}")
        
        return jsonify(photos)
    except Exception as e:
        logger.error(f"Error getting maintenance photos: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/photos/maintenance/<int:photo_number>', methods=['DELETE'])
def delete_maintenance_photo(photo_number):
    """Delete a maintenance performance photo"""
    try:
        today = date.today().isoformat()
        
        if os.path.exists(app.config['UPLOAD_FOLDER']):
            all_files = os.listdir(app.config['UPLOAD_FOLDER'])
            
            # FIXED: Match the actual filename pattern - look for today's photo first
            photo_files = [
                f for f in all_files 
                if f.startswith(f'maintenance_{today}_photo{photo_number}_')
            ]
            
            # If no photo for today, get the most recent one
            if not photo_files:
                photo_files = [
                    f for f in all_files 
                    if f'photo{photo_number}_' in f and f.startswith('maintenance_')
                ]
            
            if photo_files:
                photo_files.sort(reverse=True)
                file_to_delete = photo_files[0]
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], file_to_delete)
                os.remove(filepath)
                
                logger.info(f"Deleted maintenance photo {photo_number}: {file_to_delete}")
                return jsonify({
                    'success': True,
                    'message': f'Photo {photo_number} deleted successfully',
                    'deletedFile': file_to_delete
                })
            else:
                return jsonify({'error': 'Photo not found'}), 404
        else:
            return jsonify({'error': 'Upload folder not found'}), 404
            
    except Exception as e:
        logger.error(f"Error deleting maintenance photo: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/production/ecarts', methods=['GET'])
def get_ecarts():
    """
    Get all e-carts with their offline status from the database
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query joins the Ecart table with offline status
        # OPTION 1: If you added IsOffline column to Ecart table
        query = """
        SELECT 
            e.ecartID,
            e.ecart,
            e.costCenterID,
            e.costCenterDetail,
            ISNULL(e.IsOffline, 0) as IsOffline,
            e.OfflineUpdatedAt,
            e.OfflineUpdatedBy
        FROM [G0X_Production].[dbo].[Ecart] e
        WHERE e.Status = 1
        ORDER BY e.ecartID
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        ecarts = []
        for row in rows:
            ecarts.append({
                'ecartID': int(row.ecartID),
                'ecart': int(row.ecart),
                'costCenterID': int(row.costCenterID),
                'costCenterDetail': str(row.costCenterDetail),
                'isOffline': bool(row.IsOffline),
                'offlineUpdatedAt': row.OfflineUpdatedAt.isoformat() if row.OfflineUpdatedAt else None,
                'offlineUpdatedBy': int(row.OfflineUpdatedBy) if row.OfflineUpdatedBy else None
            })
        
        cursor.close()
        conn.close()
        
        logger.info(f"Retrieved {len(ecarts)} e-carts with offline status")
        return jsonify(ecarts)
        
    except Exception as e:
        logger.error(f"Error retrieving e-carts data: {str(e)}")
        return jsonify({'error': 'Failed to retrieve e-carts data'}), 500
    

@app.route('/api/production/ecarts/toggle/<int:ecart_id>', methods=['POST'])
def toggle_ecart_offline_status(ecart_id):
    """
    Toggle the offline status of a specific e-cart in the database
    """
    try:
        user_id = session.get('userID')  # Get current user ID
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # OPTION 1: If you added IsOffline column to Ecart table
        # First, get current status
        cursor.execute("""
            SELECT IsOffline FROM [G0X_Production].[dbo].[Ecart]
            WHERE ecartID = ?
        """, (ecart_id,))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({'error': 'E-cart not found'}), 404
        
        current_status = bool(result.IsOffline) if result.IsOffline is not None else False
        new_status = not current_status
        
        # Update the status
        cursor.execute("""
            UPDATE [G0X_Production].[dbo].[Ecart]
            SET IsOffline = ?,
                OfflineUpdatedAt = GETDATE(),
                OfflineUpdatedBy = ?
            WHERE ecartID = ?
        """, (new_status, user_id, ecart_id))
        
        
        
        conn.commit()
        cursor.close()
        conn.close()
        
        status_text = 'offline' if new_status else 'online'
        logger.info(f"E-cart {ecart_id} marked as {status_text} by user {user_id}")
        
        return jsonify({
            'success': True,
            'ecartID': ecart_id,
            'isOffline': new_status,
            'message': f'E-cart marked as {status_text}'
        })
        
    except Exception as e:
        logger.error(f"Error toggling e-cart status: {str(e)}")
        return jsonify({'error': 'Failed to toggle e-cart status'}), 500
    

@app.route('/api/production/ecarts/bulk-update', methods=['POST'])
def bulk_update_ecart_status():
    """
    Bulk update offline status for multiple e-carts
    """
    try:
        data = request.get_json()
        ecart_ids = data.get('ecartIDs', [])
        is_offline = data.get('isOffline', False)
        user_id = session.get('userID')
        
        if not ecart_ids:
            return jsonify({'error': 'No e-cart IDs provided'}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # OPTION 1: If you added IsOffline column to Ecart table
        placeholders = ','.join(['?' for _ in ecart_ids])
        query = f"""
            UPDATE [G0X_Production].[dbo].[Ecart]
            SET IsOffline = ?,
                OfflineUpdatedAt = GETDATE(),
                OfflineUpdatedBy = ?
            WHERE ecartID IN ({placeholders})
        """
        
        params = [is_offline, user_id] + ecart_ids
        cursor.execute(query, params)
    
        
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        
        status_text = 'offline' if is_offline else 'online'
        logger.info(f"Bulk updated {affected_rows} e-carts to {status_text} by user {user_id}")
        
        return jsonify({
            'success': True,
            'affectedCount': affected_rows,
            'message': f'{affected_rows} e-carts marked as {status_text}'
        })
        
    except Exception as e:
        logger.error(f"Error in bulk update: {str(e)}")
        return jsonify({'error': 'Failed to bulk update e-carts'}), 500
    

@app.route('/api/production/ecarts/mark-all-online', methods=['POST'])
def mark_all_ecarts_online():
    """
    Mark all e-carts as online
    """
    try:
        user_id = session.get('userID')
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # OPTION 1: If you added IsOffline column to Ecart table
        cursor.execute("""
            UPDATE [G0X_Production].[dbo].[Ecart]
            SET IsOffline = 0,
                OfflineUpdatedAt = GETDATE(),
                OfflineUpdatedBy = ?
            WHERE Status = 1
        """, (user_id,))
        
        
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        
        logger.info(f"All e-carts marked as online by user {user_id}")
        
        return jsonify({
            'success': True,
            'affectedCount': affected_rows,
            'message': 'All e-carts marked as online'
        })
        
    except Exception as e:
        logger.error(f"Error marking all e-carts online: {str(e)}")
        return jsonify({'error': 'Failed to mark all e-carts online'}), 500



@app.route('/api/hr/associates')
def get_hr_associates():
    """API endpoint for HR associates from HR_Associates table"""
    try:
        conn = get_db_connection_1()
        cursor = conn.cursor()
        
        query = """
        SELECT PersonalNumber, Name 
        FROM dbo.HR_Associates 
        ORDER BY Name
        """
        cursor.execute(query)
        
        associates = []
        for row in cursor.fetchall():
            associates.append({
                'personalNumber': str(row[0]) if row[0] else '',
                'name': str(row[1]) if row[1] else ''
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(associates)
        
    except Exception as e:
        logger.error(f"Error getting HR associates: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/production/downtime/weld')
def weld_downtime():
    """API endpoint for weld downtime (from WELD_Production database)"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        days = request.args.get('days', 7, type=int)
        events = production_db.get_weld_downtime_events(days, 'all')
        return jsonify(events)
    except Exception as e:
        logger.error(f"API error in weld_downtime: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Optional: Add a maintenance summary endpoint for dashboard integration
@app.route('/api/maintenance/summary')
def maintenance_summary():
    """API endpoint for maintenance summary statistics"""
    try:
        today = datetime.now().date()
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        
        try:
            # Get summary statistics
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN Category = 'TeamPerformance' THEN 1 END) as team_count,
                    AVG(CASE WHEN Category = 'TeamPerformance' AND Score IS NOT NULL THEN Score END) as avg_team_score,
                    COUNT(CASE WHEN Category = 'CribZeroStock' THEN 1 END) as zero_stock_items,
                    SUM(CASE WHEN Category = 'WorkOrder' THEN CompletedPMs ELSE 0 END) as completed_pms,
                    AVG(CASE WHEN Category = 'WorkOrder' AND ScorePercentage IS NOT NULL THEN ScorePercentage END) as work_order_score
                FROM dbo.DailyMaintenance 
                WHERE Date = ? AND IsActive = 1
            """, today)
            
            row = cursor.fetchone()
            
            summary = {
                'team_members_active': int(row[0]) if row[0] is not None else 0,
                'average_team_score': float(row[1]) if row[1] is not None else 0.0,
                'zero_stock_items': int(row[2]) if row[2] is not None else 0,
                'completed_pms_today': int(row[3]) if row[3] is not None else 0,
                'work_order_completion_rate': float(row[4]) if row[4] is not None else 0.0,
                'maintenance_efficiency': float(row[1]) if row[1] is not None else 0.0,  # Use avg team score as efficiency
                'date': today.isoformat()
            }
            
            return jsonify(summary)
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in maintenance_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

    
    

# Update the API endpoint to use the new methods
@app.route('/api/production/summary')
def production_summary():
    """API endpoint for production summary including labor data"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        # Make sure we're calling the correct method name
        summary = production_db.get_daily_production_summary()
        logger.info(f"Production summary retrieved: total_labor_hours={summary.get('total_labor_hours', 0)}")
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in production_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500



# Maintenance Performance CRUD Routes (using actual table structure)
@app.route('/api/maintenance/daily', methods=['POST'])
def add_maintenance_record():
    """Add a new maintenance record to DailyMaintenance table"""
    try:
        record_data = request.get_json()
        
        # Validate required fields
        required_fields = ['date', 'category']
        for field in required_fields:
            if not record_data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Convert date string to proper format
        record_date = record_data.get('date')
        if record_date:
            try:
                if 'T' in record_date:
                    date_obj = datetime.fromisoformat(record_date.replace('Z', '+00:00')).date()
                else:
                    date_obj = datetime.strptime(record_date, '%Y-%m-%d').date()
            except (ValueError, KeyError):
                return jsonify({'error': 'Invalid date format'}), 400
        else:
            date_obj = date.today()
        
        # Insert into database with actual column names
        query = """
        INSERT INTO dbo.DailyMaintenance (
            Date, Category, Name, HoursLoggedInSAP, HoursActualInPlant, 
            CompletedPMs, Score, SAP_Number, ItemDescription, StationUsed,
            QtyInStock, QtyOnOrder, ExpectedArrival, Time, CompanyName,
            ScopeOfWork, IsEscorted, Headcount, WorkOrderNumber,
            WeeklyReleaseTotal, ScorePercentage, Status, Notes, 
            CreatedDate, IsActive
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)
        """
        
        params = [
            date_obj,
            record_data.get('category', ''),
            record_data.get('name', ''),
            float(record_data.get('hoursLoggedInSAP', 0)) if record_data.get('hoursLoggedInSAP') else None,
            float(record_data.get('hoursActualInPlant', 0)) if record_data.get('hoursActualInPlant') else None,
            int(record_data.get('completedPMs', 0)) if record_data.get('completedPMs') else None,
            float(record_data.get('score', 0)) if record_data.get('score') else None,
            record_data.get('sapNumber', ''),
            record_data.get('itemDescription', ''),
            record_data.get('stationUsed', ''),
            int(record_data.get('qtyInStock', 0)) if record_data.get('qtyInStock') else None,
            int(record_data.get('qtyOnOrder', 0)) if record_data.get('qtyOnOrder') else None,
            record_data.get('expectedArrival') if record_data.get('expectedArrival') else None,
            record_data.get('time'),
            record_data.get('companyName', ''),
            record_data.get('scopeOfWork', ''),
            record_data.get('isEscorted'),
            int(record_data.get('headcount', 0)) if record_data.get('headcount') else None,
            record_data.get('workOrderNumber', ''),
            int(record_data.get('weeklyReleaseTotal', 0)) if record_data.get('weeklyReleaseTotal') else None,
            float(record_data.get('scorePercentage', 0)) if record_data.get('scorePercentage') else None,
            record_data.get('status', ''),
            record_data.get('notes', ''),
            bool(record_data.get('isActive', True))
        ]
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Added maintenance record for {date_obj}")
        
        return jsonify({
            'success': True, 
            'message': 'Maintenance record added successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"Error adding maintenance record: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/maintenance/daily/<path:record_key>', methods=['GET'])
def get_maintenance_record(record_key):
    """Get a specific maintenance record by composite key (date_category_identifier)"""
    try:
        # Parse the composite key: date_category_identifier
        parts = record_key.split('_', 2)
        if len(parts) < 2:
            return jsonify({'error': 'Invalid record key format'}), 400
            
        record_date = parts[0]
        category = parts[1]
        identifier = parts[2] if len(parts) > 2 else ''
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        
        # Query using composite key
        if identifier:
            query = """SELECT * FROM dbo.DailyMaintenance 
                      WHERE Date = ? AND Category = ? 
                      AND (Name = ? OR WorkOrderNumber = ? OR ItemDescription = ?)"""
            cursor.execute(query, [record_date, category, identifier, identifier, identifier])
        else:
            query = """SELECT * FROM dbo.DailyMaintenance 
                      WHERE Date = ? AND Category = ?"""
            cursor.execute(query, [record_date, category])
        
        row = cursor.fetchone()
        
        if not row:
            return jsonify({'error': 'Record not found'}), 404
        
        # Convert row to dictionary
        columns = [desc[0] for desc in cursor.description]
        record = dict(zip(columns, row))
        
        # Convert datetime objects to strings
        for key, value in record.items():
            if hasattr(value, 'isoformat'):
                record[key] = value.isoformat()
            elif value is None:
                record[key] = None
        
        cursor.close()
        conn.close()
        
        return jsonify(record)
        
    except Exception as e:
        logger.error(f"Error getting maintenance record: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/maintenance/daily/<path:record_key>', methods=['PUT'])
def update_maintenance_record(record_key):
    """Update an existing maintenance record using composite key"""
    try:
        record_data = request.get_json()
        
        # Parse the composite key
        parts = record_key.split('_', 2)
        if len(parts) < 2:
            return jsonify({'error': 'Invalid record key format'}), 400
            
        original_date = parts[0]
        original_category = parts[1]
        identifier = parts[2] if len(parts) > 2 else ''
        
        # Convert new date
        record_date = record_data.get('date')
        if record_date:
            try:
                if 'T' in record_date:
                    date_obj = datetime.fromisoformat(record_date.replace('Z', '+00:00')).date()
                else:
                    date_obj = datetime.strptime(record_date, '%Y-%m-%d').date()
            except (ValueError, KeyError):
                return jsonify({'error': 'Invalid date format'}), 400
        else:
            date_obj = datetime.strptime(original_date, '%Y-%m-%d').date()
        
        # Build WHERE clause for composite key
        if identifier:
            where_clause = """WHERE Date = ? AND Category = ? 
                             AND (Name = ? OR WorkOrderNumber = ? OR ItemDescription = ?)"""
            where_params = [original_date, original_category, identifier, identifier, identifier]
        else:
            where_clause = """WHERE Date = ? AND Category = ?"""
            where_params = [original_date, original_category]
        
        # Update database
        query = f"""
        UPDATE dbo.DailyMaintenance SET 
            Date = ?, Category = ?, Name = ?, HoursLoggedInSAP = ?, HoursActualInPlant = ?, 
            CompletedPMs = ?, Score = ?, SAP_Number = ?, ItemDescription = ?, StationUsed = ?,
            QtyInStock = ?, QtyOnOrder = ?, ExpectedArrival = ?, Time = ?, CompanyName = ?,
            ScopeOfWork = ?, IsEscorted = ?, Headcount = ?, WorkOrderNumber = ?,
            WeeklyReleaseTotal = ?, ScorePercentage = ?, Status = ?, Notes = ?, IsActive = ?
        {where_clause}
        """
        
        update_params = [
            date_obj,
            record_data.get('category', ''),
            record_data.get('name', ''),
            float(record_data.get('hoursLoggedInSAP', 0)) if record_data.get('hoursLoggedInSAP') else None,
            float(record_data.get('hoursActualInPlant', 0)) if record_data.get('hoursActualInPlant') else None,
            int(record_data.get('completedPMs', 0)) if record_data.get('completedPMs') else None,
            float(record_data.get('score', 0)) if record_data.get('score') else None,
            record_data.get('sapNumber', ''),
            record_data.get('itemDescription', ''),
            record_data.get('stationUsed', ''),
            int(record_data.get('qtyInStock', 0)) if record_data.get('qtyInStock') else None,
            int(record_data.get('qtyOnOrder', 0)) if record_data.get('qtyOnOrder') else None,
            record_data.get('expectedArrival') if record_data.get('expectedArrival') else None,
            record_data.get('time'),
            record_data.get('companyName', ''),
            record_data.get('scopeOfWork', ''),
            record_data.get('isEscorted'),
            int(record_data.get('headcount', 0)) if record_data.get('headcount') else None,
            record_data.get('workOrderNumber', ''),
            int(record_data.get('weeklyReleaseTotal', 0)) if record_data.get('weeklyReleaseTotal') else None,
            float(record_data.get('scorePercentage', 0)) if record_data.get('scorePercentage') else None,
            record_data.get('status', ''),
            record_data.get('notes', ''),
            bool(record_data.get('isActive', True))
        ] + where_params
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        cursor.execute(query, update_params)
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_affected > 0:
            logger.info(f"Updated maintenance record {record_key}")
            return jsonify({
                'success': True,
                'message': 'Maintenance record updated successfully'
            })
        else:
            return jsonify({'error': 'Record not found or no changes made'}), 404
            
    except Exception as e:
        logger.error(f"Error updating maintenance record: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/maintenance/daily/<path:record_key>', methods=['DELETE'])
def delete_maintenance_record(record_key):
    """Delete a maintenance record using composite key"""
    try:
        # Parse the composite key
        parts = record_key.split('_', 2)
        if len(parts) < 2:
            return jsonify({'error': 'Invalid record key format'}), 400
            
        record_date = parts[0]
        category = parts[1]
        identifier = parts[2] if len(parts) > 2 else ''
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        
        # Build WHERE clause for composite key
        if identifier:
            query = """DELETE FROM dbo.DailyMaintenance 
                      WHERE Date = ? AND Category = ? 
                      AND (Name = ? OR WorkOrderNumber = ? OR ItemDescription = ?)"""
            cursor.execute(query, [record_date, category, identifier, identifier, identifier])
        else:
            query = """DELETE FROM dbo.DailyMaintenance 
                      WHERE Date = ? AND Category = ?"""
            cursor.execute(query, [record_date, category])
        
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_affected > 0:
            logger.info(f"Deleted maintenance record {record_key}")
            return jsonify({
                'success': True,
                'message': 'Maintenance record deleted successfully'
            })
        else:
            return jsonify({'error': 'Record not found'}), 404
            
    except Exception as e:
        logger.error(f"Error deleting maintenance record: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Updated endpoint to get maintenance data with composite keys
@app.route('/api/maintenance/daily')
def get_maintenance_data_with_keys():
    """API endpoint for daily maintenance data with composite keys"""
    try:
        conn = get_db_connection_1()
        cursor = conn.cursor()
        
        try:
            # Get all records from the last 30 days instead of just today
            cursor.execute("""
                SELECT Date, Category, Name, HoursLoggedInSAP, HoursActualInPlant, CompletedPMs, Score, 
                       SAP_Number, ItemDescription, StationUsed, QtyInStock, QtyOnOrder, 
                       ExpectedArrival, Time, CompanyName, ScopeOfWork, IsEscorted, Headcount,
                       WorkOrderNumber, WeeklyReleaseTotal, ScorePercentage, Status, Notes,
                       CreatedDate, IsActive
                FROM dbo.DailyMaintenance 
                WHERE Date >= DATEADD(day, -30, GETDATE()) AND IsActive = 1
                ORDER BY CreatedDate DESC
            """)
            
            all_records = []
            for row in cursor.fetchall():
                # Create composite key for this record
                identifier = row[2] or row[18] or row[8] or str(len(all_records))  # Name, WorkOrderNumber, ItemDescription, or index
                record_key = f"{row[0]}_{row[1]}_{identifier}"
                
                record = {
                    'recordKey': record_key,  # Our composite key
                    'date': str(row[0]) if row[0] else '',
                    'category': str(row[1]) if row[1] else '',
                    'name': str(row[2]) if row[2] else '',
                    'hoursLoggedInSAP': float(row[3]) if row[3] is not None else 0.0,
                    'hoursActualInPlant': float(row[4]) if row[4] is not None else 0.0,
                    'completedPMs': int(row[5]) if row[5] is not None else 0,
                    'score': float(row[6]) if row[6] is not None else 0.0,
                    'sapNumber': str(row[7]) if row[7] else '',
                    'itemDescription': str(row[8]) if row[8] else '',
                    'stationUsed': str(row[9]) if row[9] else '',
                    'qtyInStock': int(row[10]) if row[10] is not None else 0,
                    'qtyOnOrder': int(row[11]) if row[11] is not None else 0,
                    'expectedArrival': row[12].isoformat() if row[12] else None,
                    'time': str(row[13]) if row[13] else '',
                    'companyName': str(row[14]) if row[14] else '',
                    'scopeOfWork': str(row[15]) if row[15] else '',
                    'isEscorted': bool(row[16]) if row[16] is not None else False,
                    'headcount': int(row[17]) if row[17] is not None else 0,
                    'workOrderNumber': str(row[18]) if row[18] else '',
                    'weeklyReleaseTotal': int(row[19]) if row[19] is not None else 0,
                    'scorePercentage': float(row[20]) if row[20] is not None else 0.0,
                    'status': str(row[21]) if row[21] else '',
                    'notes': str(row[22]) if row[22] else '',
                    'createdDate': row[23].isoformat() if row[23] else '',
                    'isActive': bool(row[24]) if row[24] is not None else True
                }
                all_records.append(record)
            
            # Group records by category for backward compatibility
            result = {
                'teamPerformance': [r for r in all_records if r['category'] == 'TeamPerformance'],
                'cribZeroStock': [r for r in all_records if r['category'] == 'CribZeroStock'],
                'toner': [r for r in all_records if r['category'] == 'Toner'],
                'grease': [r for r in all_records if r['category'] == 'Grease'],
                'contractor': [r for r in all_records if r['category'] == 'Contractor'],
                'workOrder': [r for r in all_records if r['category'] == 'WorkOrder'],
                'allRecords': all_records
            }
            
            return jsonify(result)
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in get_maintenance_data_with_keys: {str(e)}")
        return jsonify({'error': str(e)}), 500
    



def get_downtime_by_category(self, days=7):
        """Get downtime events grouped by fourM category from G0X_Production database"""
        try:
            conn = None
            try:
                conn = get_db_connection()  # This connects to G0X_Production
                
                query = """
                SELECT 
                    fourM,
                    COUNT(*) as event_count,
                    SUM(CASE 
                        WHEN endTime IS NOT NULL THEN DATEDIFF(MINUTE, startTime, endTime)
                        ELSE DATEDIFF(MINUTE, startTime, GETDATE())
                    END) as total_minutes,
                    AVG(CASE 
                        WHEN endTime IS NOT NULL THEN DATEDIFF(MINUTE, startTime, endTime)
                        ELSE DATEDIFF(MINUTE, startTime, GETDATE())
                    END) as avg_minutes
                FROM [dbo].[Downtime]
                WHERE startTime >= DATEADD(day, ?, GETDATE())
                GROUP BY fourM
                ORDER BY total_minutes DESC
                """
                
                df = pd.read_sql(query, conn, params=[-days])
                
                categories = []
                for _, row in df.iterrows():
                    four_m = str(row.get('fourM', ''))
                    
                    # Categorize
                    if four_m in ['MAN', 'METHOD']:
                        category_type = 'Production'
                    elif four_m == 'MACHINE':
                        category_type = 'Maintenance'
                    else:
                        category_type = 'Other'
                    
                    categories.append({
                        'fourM': four_m,
                        'categoryType': category_type,
                        'eventCount': int(row.get('event_count', 0)),
                        'totalMinutes': int(row.get('total_minutes', 0)),
                        'avgMinutes': round(float(row.get('avg_minutes', 0)), 1)
                    })
                
                return categories
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting downtime by category: {str(e)}")
            return []

@app.route('/api/production/daily', methods=['POST'])
def add_daily_production():
    """API endpoint for adding/updating daily production with labor data (UPSERT)"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        production_data = request.get_json()
        
        # Validate required fields
        required_fields = ['productionDate']
        for field in required_fields:
            if not production_data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Add created by from session if available
        if session.get('name'):
            production_data['createdBy'] = session['name']
        else:
            production_data['createdBy'] = 'System'
        
        # Use upsert method instead of add
        success = production_db.upsert_daily_production(production_data)
        
        # Optional: Clean up old records (keep only last 90 days)
        try:
            production_db.cleanup_old_production_records(90)
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup old records: {cleanup_error}")
            # Don't fail the main operation for cleanup issues
        
        return jsonify({
            'success': True, 
            'message': 'Daily production record saved successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"API error in add/update daily production: {str(e)}")
        return jsonify({'error': str(e)}), 500
# 2. Add these new API endpoints after your existing production endpoints:

@app.route('/api/production/downtime/summary')
def downtime_summary():
    """API endpoint for downtime summary"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        summary = production_db.get_downtime_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in downtime_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/production/downtime/events')
def downtime_events():
    """API endpoint for downtime events"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        days = request.args.get('days', 7, type=int)
        downtime_type = request.args.get('type', 'all')  # 'all', 'production', 'maintenance'
        
        events = production_db.get_downtime_events(days, downtime_type)
        return jsonify(events)
    except Exception as e:
        logger.error(f"API error in downtime_events: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/production/downtime/categories')
def downtime_categories():
    """API endpoint for downtime by category"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        days = request.args.get('days', 7, type=int)
        categories = production_db.get_downtime_by_category(days)
        return jsonify(categories)
    except Exception as e:
        logger.error(f"API error in downtime_categories: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/production/downtime/production')
def production_downtime():
    """API endpoint for production downtime (MAN and METHOD)"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        days = request.args.get('days', 7, type=int)
        events = production_db.get_downtime_events(days, 'production')
        return jsonify(events)
    except Exception as e:
        logger.error(f"API error in production_downtime: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/production/downtime/maintenance')
def maintenance_downtime():
    """API endpoint for maintenance downtime (MACHINE)"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        days = request.args.get('days', 7, type=int)
        events = production_db.get_downtime_events(days, 'maintenance')
        return jsonify(events)
    except Exception as e:
        logger.error(f"API error in maintenance_downtime: {str(e)}")
        return jsonify({'error': str(e)}), 500




@app.route('/api/production/health')
def production_health_check():
    """Production system health check endpoint"""
    try:
        if production_db:
            conn = production_db.get_connection()
            conn.close()
            return jsonify({'status': 'healthy', 'production_database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'production_database': 'disconnected'}), 503
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503
    

@app.route('/api/production/jph-pplhdit')
def production_jph_pplhdit():
    """API endpoint for JPH and PPLHDIT data"""
    if not production_db:
        return jsonify({'error': 'Production database not available'}), 503
    
    try:
        limit = request.args.get('limit', 4, type=int)
        data = production_db.get_jph_pplhdit_data(limit)
        return jsonify(data)
    except Exception as e:
        logger.error(f"API error in jph_pplhdit: {str(e)}")
        return jsonify({'error': str(e)}), 500



@app.route('/production')
def production_dashboard_page():
    """Production dashboard page"""
    return render_template('production.html')

@app.route('/production-management')
@require_production_access
def production_management_page():
    """Production management page"""
    return render_template('production_management.html')


# Simplified Logistics Database Class - Reading from existing tables
class LogisticsDatabase:
    def __init__(self):
        try:
            # Test both connections
            conn_prod = get_db_connection()  # Production for downtime
            conn_prod.close()
            conn_test = get_db_connection_1()  # Test for suppliers
            conn_test.close()
            logger.info("LogisticsDatabase initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize LogisticsDatabase: {str(e)}")
            raise
    
    def get_production_connection(self):
        return get_db_connection()  # Production database for downtime
    
    def get_test_connection(self):
        return get_db_connection_1()  # Test database for suppliers

    def get_suppliers_with_logos(self):
        """Get suppliers from Logistics_Planner table with logo support"""
        try:
            conn = None
            try:
                conn = get_db_connection_1()  # Test database (G0X_Production_Test)
                query = """
                SELECT VendorID, Vendor FROM dbo.Logistics_Planner 
                WHERE Vendor IS NOT NULL AND Vendor != ''
                ORDER BY Vendor
                """
                df = pd.read_sql(query, conn)
                
                suppliers = []
                for _, row in df.iterrows():
                    vendor_id = row.get('VendorID', '')
                    vendor_name = row.get('Vendor', '')
                    
                    if pd.notna(vendor_name) and str(vendor_name).strip():
                        suppliers.append({
                            'id': str(vendor_id) if pd.notna(vendor_id) else '',
                            'name': str(vendor_name).strip(),
                            'logoPath': f"/static/vendor-logos/{vendor_id}.png"
                        })
                
                logger.info(f"Found {len(suppliers)} suppliers with logos")
                return suppliers
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting suppliers with logos: {str(e)}")
            return []
    
    def execute_query_production(self, query, params=None):
        """Execute query on production database"""
        conn = None
        try:
            conn = self.get_production_connection()
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Production logistics query failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query_test(self, query, params=None):
        """Execute query on test database"""
        conn = None
        try:
            conn = self.get_test_connection()
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Test logistics query failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()

    

    def execute_non_query_test(self, query, params=None):
        """Execute non-query on test database"""
        conn = None
        cursor = None
        try:
            conn = self.get_test_connection()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Test logistics non-query failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _get_time_filter_for_daily(self):
        """
        Get time filter for daily queries.
        - Monday: last 72 hours (3 days)
        - Other days: last 24 hours (1 day)
        """
        today = datetime.now().weekday()  # Monday = 0, Sunday = 6
        if today == 0:  # Monday
            return -72  # Last 72 hours on Monday
        else:
            return -24  # Last 24 hours on other days

    def _get_time_filter_for_reports(self):
        """
        Get time filter for report queries (non-daily).
        - Normal days: last 72 hours (3 days)
        - Monday: last 72 hours (3 days) 
        """
        today = datetime.now().weekday()  # Monday = 0, Sunday = 6
        if today == 0:  # Monday
            return -72  # Last 72 hours
        else:
            return -24  # Always last 3 days for reports

    def _build_logistics_filter(self):
        """Build the WHERE clause for logistics-related downtime"""
        # Removed overly broad keywords that match non-logistics events
        logistics_keywords = [
            'supply', 'logistics', 'replenishment', 
            'inventory', 'racking', 'warehouse', 'storage', 'pallet', 'shelves',
            'truck', 'trucks', 'supplier', 'racks', 'rack', 'forklift', 
            'forklifts', 'logistic', 'transportation', 'freight', 
            'distribution', 'conveyor', 'shipping', 'material shortage'
        ]
        
        # Build the keyword filter for the reason field
        keyword_conditions = []
        for keyword in logistics_keywords:
            keyword_conditions.append(f"reason LIKE '%{keyword}%'")
        
        keyword_filter = " OR ".join(keyword_conditions)
        
        # Simplified filter to avoid triple counting:
        # Either fourM = 'MATERIAL' OR Logistic = 1 OR keywords match
        complete_filter = f"(fourM = 'MATERIAL' OR Logistic = 1 OR ({keyword_filter}))"
        
        return complete_filter

    def get_logistics_summary(self):
        """Get logistics summary from downtime and suppliers tables - FIXED VERSION"""
        try:
            summary = {}
            
            # Build logistics filter
            logistics_filter = self._build_logistics_filter()
            
            # FIXED: Get material-related downtime from SELECTED items only (no time filter)
            material_downtime_query = f"""
            SELECT 
                COUNT(*) as total_events,
                SUM(CASE 
                    WHEN d.endTime IS NOT NULL THEN DATEDIFF(MINUTE, d.startTime, d.endTime)
                    ELSE DATEDIFF(MINUTE, d.startTime, GETDATE())
                END) as total_minutes
            FROM dbo.Downtime d
            INNER JOIN dbo.Logistic_Downtime ld ON d.downtimeID = ld.downtimeID
            WHERE {logistics_filter}
            AND d.endTime IS NOT NULL
            """
            material_df = self.execute_query_production(material_downtime_query)
            
            # Extract values safely
            if not material_df.empty and pd.notna(material_df.iloc[0]['total_minutes']):
                summary['material_downtime_minutes'] = int(material_df.iloc[0]['total_minutes'])
            else:
                summary['material_downtime_minutes'] = 0
                
            if not material_df.empty:
                summary['material_issues_today'] = int(material_df.iloc[0]['total_events'])
            else:
                summary['material_issues_today'] = 0
            
            # Get ALL suppliers count from TEST database (TOTAL SUPPLIERS)
            total_suppliers_query = """
            SELECT COUNT(*) as count FROM dbo.DailySuppliers
            """
            total_suppliers_df = self.execute_query_test(total_suppliers_query)
            summary['total_suppliers'] = int(total_suppliers_df.iloc[0]['count']) if not total_suppliers_df.empty else 0
            summary['active_suppliers'] = summary['total_suppliers']  # Same value for compatibility
            
            # **NEW: Get project breakdown**
            project_query = """
            SELECT 
            LTRIM(RTRIM(value)) as Project,
            COUNT(*) as count
            FROM dbo.DailySuppliers
            CROSS APPLY STRING_SPLIT(ISNULL(Project, ''), ',')
            WHERE LTRIM(RTRIM(value)) != '' 
            AND LTRIM(RTRIM(value)) != 'N/A'
            AND value IS NOT NULL
            GROUP BY LTRIM(RTRIM(value))
            ORDER BY count DESC
            """
            project_df = self.execute_query_test(project_query)
            
            if not project_df.empty:
                summary['project_breakdown'] = [
                    {'project': row['Project'], 'count': int(row['count'])}
                    for _, row in project_df.iterrows()
                ]
            else:
                summary['project_breakdown'] = []
            
            # Get high risk suppliers from TEST database
            high_risk_query = """
            SELECT COUNT(*) as count FROM dbo.DailySuppliers
            WHERE Risk = 'High'
            """
            high_risk_df = self.execute_query_test(high_risk_query)
            summary['high_risk_suppliers'] = int(high_risk_df.iloc[0]['count']) if not high_risk_df.empty else 0
            summary['supplier_issues_count'] = summary['high_risk_suppliers']  # Alias for compatibility
            
            # Get medium risk suppliers from TEST database
            medium_risk_query = """
            SELECT COUNT(*) as count FROM dbo.DailySuppliers
            WHERE Risk = 'Medium'
            """
            medium_risk_df = self.execute_query_test(medium_risk_query)
            summary['medium_risk_suppliers'] = int(medium_risk_df.iloc[0]['count']) if not medium_risk_df.empty else 0
            
            # Get low risk suppliers from TEST database
            low_risk_query = """
            SELECT COUNT(*) as count FROM dbo.DailySuppliers
            WHERE Risk = 'Low'
            """
            low_risk_df = self.execute_query_test(low_risk_query)
            summary['low_risk_suppliers'] = int(low_risk_df.iloc[0]['count']) if not low_risk_df.empty else 0
            
            # Get logistics events (last 3 days for reports, use Monday logic) from PRODUCTION database
            today = datetime.now().weekday()  # Monday = 0, Sunday = 6
            if today == 0:  # Monday
                report_hours = -72  # Last 72 hours on Monday
            else:
                report_hours = -72  # Last 72 hours normally
                
            logistics_events_query = f"""
            SELECT COUNT(*) as count FROM dbo.Downtime 
            WHERE {logistics_filter}
            AND startTime >= DATEADD(hour, ?, GETDATE())
            AND endTime IS NOT NULL
            """
            logistics_df = self.execute_query_production(logistics_events_query, params=[report_hours])
            summary['logistics_events_count'] = int(logistics_df.iloc[0]['count']) if not logistics_df.empty else 0
            
            # Calculate mock delivery performance based on downtime
            base_performance = 95.0
            performance_impact = min(10.0, summary['material_downtime_minutes'] / 30.0)  # 1% per 30 min
            summary['delivery_performance'] = max(80.0, base_performance - performance_impact)
            
            # Mock additional metrics for dashboard compatibility
            summary['active_shipments'] = summary['logistics_events_count']
            summary['pending_deliveries'] = max(0, summary['material_issues_today'])
            summary['inventory_alerts'] = summary['material_issues_today']
            summary['on_time_delivery_rate'] = summary['delivery_performance']
            summary['warehouse_capacity_usage'] = 78.5
            
            logger.info(f"Logistics summary generated - material_downtime_minutes: {summary['material_downtime_minutes']}, total_suppliers: {summary['total_suppliers']}, high_risk: {summary['high_risk_suppliers']}, medium_risk: {summary['medium_risk_suppliers']}, low_risk: {summary['low_risk_suppliers']}, projects: {len(summary['project_breakdown'])}")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating logistics summary: {str(e)}")
            # Return default values on error
            return {
                'material_downtime_minutes': 0,
                'total_suppliers': 0,
                'supplier_issues_count': 0,
                'high_risk_suppliers': 0,
                'medium_risk_suppliers': 0,
                'low_risk_suppliers': 0,
                'logistics_events_count': 0,
                'delivery_performance': 95.0,
                'active_suppliers': 0,
                'material_issues_today': 0,
                'active_shipments': 0,
                'pending_deliveries': 0,
                'inventory_alerts': 0,
                'on_time_delivery_rate': 95.0,
                'warehouse_capacity_usage': 78.5,
                'project_breakdown': []  # **ADD THIS to error return**
            }

    def get_all_downtime_with_selection(self):
        """Get all downtime events with selection status from Logistic_Downtime table"""
        try:
            # Build logistics filter
            logistics_filter = self._build_logistics_filter()
            
            query = f"""
            SELECT 
                d.downtimeID,
                d.startTime,
                d.endTime,
                d.reason,
                d.fourM as category,
                d.whoFixed,
                d.Logistic,
                d.closed,
                DATEDIFF(MINUTE, d.startTime, ISNULL(d.endTime, GETDATE())) as durationMinutes,
                CASE WHEN ld.downtimeID IS NOT NULL THEN 1 ELSE 0 END as isSelected,
                ld.ActionTaken,
                ld.Date_ as selectionDate,
                ld.[User] as selectedBy
            FROM dbo.Downtime d
            LEFT JOIN dbo.Logistic_Downtime ld ON d.downtimeID = ld.downtimeID
            WHERE {logistics_filter}
            ORDER BY d.startTime DESC
            """
            
            df = self.execute_query_production(query)
            
            events = []
            for _, row in df.iterrows():
                # Convert datetime properly
                start_time = row.get('startTime')
                end_time = row.get('endTime')
                
                start_time_str = start_time.isoformat() if pd.notna(start_time) and hasattr(start_time, 'isoformat') else 'N/A'
                end_time_str = end_time.isoformat() if pd.notna(end_time) and hasattr(end_time, 'isoformat') else 'Ongoing'
                
                # Determine status
                closed = row.get('closed')
                status = 'Resolved' if pd.notna(closed) and str(closed).lower() == 'yes' else 'Open'
                
                # Calculate duration
                duration = row.get('durationMinutes')
                duration_minutes = int(duration) if pd.notna(duration) else 0
                
                # Determine category
                four_m = str(row.get('category', ''))
                logistic_flag = row.get('Logistic')
                reason = str(row.get('reason', '')).lower()
                
                logistics_keywords = [
                    'supply', 'logistics', 'replenishment', 
                    'inventory', 'racking', 'warehouse', 'storage', 'pallet', 'shelves',
                    'truck', 'trucks', 'supplier', 'racks', 'rack', 'forklift', 
                    'forklifts', 'logistic', 'transportation', 'freight', 
                    'distribution', 'conveyor', 'shipping', 'material shortage'
                ]
                
                has_keywords = any(keyword in reason for keyword in logistics_keywords)
                
                if four_m == 'MATERIAL' and has_keywords:
                    category = '📦 Material'
                elif pd.notna(logistic_flag) and logistic_flag == 1:
                    category = '⛟ Logistics'
                elif has_keywords:
                    category = '💬 Keywords'
                else:
                    category = '✔ Other'
                
                events.append({
                    'downtimeID': str(row.get('downtimeID')),
                    'startTime': start_time_str,
                    'endTime': end_time_str,
                    'reason': str(row.get('reason', '') if pd.notna(row.get('reason')) else 'Logistics issue'),
                    'category': category,
                    'fourM': four_m,
                    'status': status,
                    'durationMinutes': duration_minutes,
                    'whoFixed': str(row.get('whoFixed', '') if pd.notna(row.get('whoFixed')) else ''),
                    'isSelected': bool(row.get('isSelected', 0)),
                    'actionTaken': str(row.get('ActionTaken', '') if pd.notna(row.get('ActionTaken')) else ''),
                    'selectionDate': row.get('selectionDate').isoformat() if pd.notna(row.get('selectionDate')) and hasattr(row.get('selectionDate'), 'isoformat') else None,
                    'selectedBy': str(row.get('selectedBy', '') if pd.notna(row.get('selectedBy')) else '')
                })
            
            logger.info(f"Retrieved {len(events)} downtime events with selection status")
            return events
            
        except Exception as e:
            logger.error(f"Error getting all downtime with selection: {str(e)}")
            return []
    
    def save_downtime_selection(self, selected_ids, user='System'):
        """Save downtime selection to Logistic_Downtime table - preserves existing records"""
        conn = None
        cursor = None
        try:
            conn = self.get_production_connection()
            cursor = conn.cursor()
            
            # Get all existing downtimeIDs in Logistic_Downtime
            cursor.execute("SELECT downtimeID FROM dbo.Logistic_Downtime")
            existing_ids = set(row[0] for row in cursor.fetchall())
            
            # Convert selected_ids to set for comparison
            selected_set = set(str(id) for id in selected_ids)
            
            # Find IDs to add (selected but not in table yet)
            ids_to_add = selected_set - existing_ids
            
            # Find IDs to remove (in table but not selected)
            ids_to_remove = existing_ids - selected_set
            
            # Remove unselected items
            if ids_to_remove:
                placeholders = ','.join('?' * len(ids_to_remove))
                cursor.execute(f"DELETE FROM dbo.Logistic_Downtime WHERE downtimeID IN ({placeholders})", 
                              list(ids_to_remove))
                logger.info(f"Removed {len(ids_to_remove)} downtime events from selection")
            
            # Add newly selected items
            if ids_to_add:
                for downtime_id in ids_to_add:
                    # Get next mainID
                    cursor.execute("SELECT ISNULL(MAX(mainID), 0) + 1 FROM dbo.Logistic_Downtime")
                    next_main_id = cursor.fetchone()[0]
                    
                    # Insert new selection
                    cursor.execute("""
                        INSERT INTO dbo.Logistic_Downtime (downtimeID, mainID, ActionTaken, Date_, [User])
                        VALUES (?, ?, ?, GETDATE(), ?)
                    """, (downtime_id, next_main_id, '', user))
                
                logger.info(f"Added {len(ids_to_add)} downtime events to selection")
            
            conn.commit()
            logger.info(f"Saved downtime selection: {len(selected_set)} total events selected by {user}")
            return True
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error saving downtime selection: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def get_material_downtime(self, days=None):
        """Get material/logistics-related downtime events - ONLY SELECTED ONES"""
        try:
            # REMOVED: Don't filter by time, just get ALL selected events
            # Build logistics filter
            logistics_filter = self._build_logistics_filter()
            
            # MODIFIED: Get ALL downtime events that are in Logistic_Downtime table (selected)
            # NO TIME FILTER - just get what's selected
            query = f"""
            SELECT 
                d.downtimeID,
                d.startTime,
                d.endTime,
                d.closed,
                d.dateClosed,
                d.reason,
                d.fourM,
                d.whoFixed,
                d.Logistic,
                CONCAT(ISNULL(mach.line, ''), '-', ISNULL(mach.machine, '')) AS machineLocation,
                CASE 
                    WHEN d.endTime IS NOT NULL THEN DATEDIFF(MINUTE, d.startTime, d.endTime)
                    ELSE DATEDIFF(MINUTE, d.startTime, GETDATE())
                END as durationMinutes,
                ld.ActionTaken,
                ld.Date_ as selectionDate
            FROM dbo.Downtime d
            INNER JOIN dbo.Logistic_Downtime ld ON d.downtimeID = ld.downtimeID
            LEFT JOIN dbo.Machine mach ON d.machineID = mach.machineID
            WHERE {logistics_filter}
            ORDER BY d.startTime DESC
            """
            
            # NO PARAMS - no time filter
            df = self.execute_query_production(query)
            
            events = []
            for _, row in df.iterrows():
                # Convert datetime properly
                start_time = row.get('startTime')
                end_time = row.get('endTime')
                
                start_time_str = start_time.isoformat() if pd.notna(start_time) and hasattr(start_time, 'isoformat') else 'N/A'
                end_time_str = end_time.isoformat() if pd.notna(end_time) and hasattr(end_time, 'isoformat') else 'Ongoing'
                
                # Determine status
                closed = row.get('closed')
                status = 'Resolved' if pd.notna(closed) and str(closed).lower() == 'yes' else 'Open'
                
                # Calculate duration
                duration = row.get('durationMinutes')
                duration_minutes = int(duration) if pd.notna(duration) else 0
                
                # Determine category
                four_m = str(row.get('fourM', ''))
                logistic_flag = row.get('Logistic')
                reason = str(row.get('reason', '')).lower()
                
                logistics_keywords = [
                    'supply', 'logistics', 'replenishment', 
                    'inventory', 'racking', 'warehouse', 'storage', 'pallet', 'shelves',
                    'truck', 'trucks', 'supplier', 'racks', 'rack', 'forklift', 
                    'forklifts', 'logistic', 'transportation', 'freight', 
                    'distribution', 'conveyor', 'shipping', 'material shortage'
                ]
                
                has_keywords = any(keyword in reason for keyword in logistics_keywords)
                
                if four_m == 'MATERIAL' and has_keywords:
                    category = '📦 Material'
                elif pd.notna(logistic_flag) and logistic_flag == 1:
                    category = '⛟'
                elif has_keywords:
                    category = '💬'
                else:
                    category = '✓'
                
                events.append({
                    'downtimeID': str(row.get('downtimeID')),
                    'eventID': f"LOG_{start_time.strftime('%Y%m%d_%H%M%S') if pd.notna(start_time) and hasattr(start_time, 'strftime') else 'UNKNOWN'}",
                    'startTime': start_time_str,
                    'endTime': end_time_str,
                    'reason': str(row.get('reason', '') if pd.notna(row.get('reason')) else 'Logistics issue'),
                    'category': category,
                    'status': status,
                    'durationMinutes': duration_minutes,
                    'resolvedBy': str(row.get('whoFixed', '') if pd.notna(row.get('whoFixed')) else ''),
                    'priority': 'High' if duration_minutes > 60 else 'Medium' if duration_minutes > 30 else 'Low',
                    'affectedArea': 'Production Line',
                    'impact': 'Production Delay',
                    'fourM': four_m,
                    'logisticFlag': str(logistic_flag) if pd.notna(logistic_flag) else '0',
                    'actionTaken': str(row.get('ActionTaken', '') if pd.notna(row.get('ActionTaken')) else ''),
                    'machineLocation': str(row.get('machineLocation', '') if pd.notna(row.get('machineLocation')) else 'N/A')
                })
            
            logger.info(f"Retrieved {len(events)} selected material downtime events (NO TIME FILTER)")
            return events
            
        except Exception as e:
            logger.error(f"Error getting material downtime: {str(e)}")
            return []

        
    def add_downtime(self, downtime_data):
        """Add a new downtime event to PRODUCTION database"""
        conn = None
        cursor = None
        try:
            conn = self.get_production_connection()
            cursor = conn.cursor()
            
            query = """
            INSERT INTO dbo.Downtime (
                startTime, endTime, reason, fourM, whoFixed, Logistic, closed
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            params = [
                downtime_data.get('startTime'),
                downtime_data.get('endTime'),
                downtime_data.get('reason'),
                downtime_data.get('fourM'),
                downtime_data.get('whoFixed'),
                downtime_data.get('logistic', 1),  # Default to 1 for logistics
                downtime_data.get('closed', 'No')
            ]
            
            cursor.execute(query, params)
            conn.commit()
            
            # Get the inserted downtimeID
            cursor.execute("SELECT @@IDENTITY AS downtimeID")
            downtime_id = cursor.fetchone()[0]
            
            logger.info(f"Added downtime event, downtimeID: {downtime_id}")
            return downtime_id
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error adding downtime: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_logistics_downtime(self, days=None):
        """Alias for get_material_downtime for compatibility"""
        return self.get_material_downtime(days)

    def get_suppliers(self):
        """Get suppliers from DailySuppliers table in TEST database"""
        try:
            query = """
            SELECT * FROM dbo.DailySuppliers 
            ORDER BY SupplierID
            """
            df = self.execute_query_test(query)
            
            suppliers = []
            for _, row in df.iterrows():
                # Convert CreatedDate properly
                created_date = row.get('CreatedDate')
                if pd.notna(created_date):
                    if hasattr(created_date, 'isoformat'):
                        created_date_str = created_date.isoformat()
                    else:
                        created_date_str = str(created_date)
                else:
                    created_date_str = 'N/A'
                
                risk_level = str(row.get('Risk', 'Medium') if pd.notna(row.get('Risk')) else 'Medium')
                
                suppliers.append({
                    'supplierID': str(row.get('SupplierID', '')),
                    'Number': str(row.get('Number', '') if pd.notna(row.get('Number')) else ''),
                    'Risk': risk_level,
                    'Actions': str(row.get('Actions', '') if pd.notna(row.get('Actions')) else ''),
                    'CreatedDate': created_date_str,
                    'CreatedBy': str(row.get('CreatedBy', '') if pd.notna(row.get('CreatedBy')) else ''),
                    # Additional fields for frontend compatibility
                    'supplierName': f"Supplier {row.get('Number', row.get('SupplierID', 'Unknown'))}",
                    'contactPerson': 'Contact Person',
                    'email': f"contact@supplier{row.get('SupplierID', '')}.com",
                    'phone': '(555) 123-4567',
                    'performanceRating': '4.2' if risk_level == 'Low' else '3.5' if risk_level == 'Medium' else '2.8',
                    'activeOrders': '5' if risk_level == 'Low' else '3' if risk_level == 'Medium' else '1'
                })
            
            return suppliers
            
        except Exception as e:
            logger.error(f"Error getting suppliers: {str(e)}")
            return []
        
    @app.route('/api/logistics/downtime-all')
    def logistics_downtime_all():
        """API endpoint for ALL downtime events (for selection interface)"""
        if not logistics_db:
            return jsonify({'error': 'Logistics database not available'}), 503
        
        try:
            events = logistics_db.get_all_downtime_with_selection()
            return jsonify(events)
        except Exception as e:
            logger.error(f"API error in logistics_downtime_all: {str(e)}")
            return jsonify({'error': str(e)}), 500

    # CRUD operations for suppliers
    def add_supplier(self, supplier_data):
        """Add a new supplier to DailySuppliers table"""
        try:
            query = """
            INSERT INTO dbo.DailySuppliers (
                Number, Risk, Actions, CreatedDate, CreatedBy
            ) VALUES (?, ?, ?, GETDATE(), ?)
            """
            
            params = [
                supplier_data.get('number', ''),
                supplier_data.get('risk', 'Medium'),
                supplier_data.get('actions', ''),
                supplier_data.get('createdBy', 'System')
            ]
            
            rows_affected = self.execute_non_query_test(query, params)
            logger.info(f"Added supplier, rows affected: {rows_affected}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding supplier: {str(e)}")
            raise

    def update_supplier(self, supplier_id, supplier_data):
        """Update an existing supplier"""
        try:
            query = """
            UPDATE dbo.DailySuppliers SET 
                Number = ?, Risk = ?, Actions = ?
            WHERE SupplierID = ?
            """
            
            params = [
                supplier_data.get('number', ''),
                supplier_data.get('risk', 'Medium'),
                supplier_data.get('actions', ''),
                supplier_id
            ]
            
            rows_affected = self.execute_non_query_test(query, params)
            logger.info(f"Updated supplier {supplier_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error updating supplier: {str(e)}")
            raise

    def delete_supplier(self, supplier_id):
        """Delete a supplier"""
        try:
            query = "DELETE FROM dbo.DailySuppliers WHERE SupplierID = ?"
            rows_affected = self.execute_non_query_test(query, [supplier_id])
            logger.info(f"Deleted supplier {supplier_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error deleting supplier: {str(e)}")
            raise

    def get_supplier_by_id(self, supplier_id):
        """Get a specific supplier by ID"""
        try:
            query = "SELECT * FROM dbo.DailySuppliers WHERE SupplierID = ?"
            df = self.execute_query_test(query, params=[supplier_id])
            
            if df.empty:
                return None
            
            row = df.iloc[0]
            
            # Convert to expected format
            supplier = {}
            for key, value in row.items():
                if pd.isna(value):
                    supplier[key] = None
                elif hasattr(value, 'isoformat'):
                    supplier[key] = value.isoformat()
                else:
                    supplier[key] = str(value) if value is not None else ''
            
            return supplier
            
        except Exception as e:
            logger.error(f"Error getting supplier by ID: {str(e)}")
            raise


# Simplified Continuous Improvement Database Class - Reading from DailyCI
class ContinuousImprovementDatabase:
    def __init__(self):
        try:
            conn = self.get_connection()
            conn.close()
            logger.info("ContinuousImprovementDatabase initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ContinuousImprovementDatabase: {str(e)}")
            raise
    
    def get_connection(self):
        return get_db_connection_1()  # Use test database for DailyCI
    
    def execute_query(self, query, params=None):
        """Execute query with proper connection handling"""
        conn = None
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"CI query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_non_query(self, query, params=None):
        """Execute non-query (INSERT, UPDATE, DELETE) with proper connection handling"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"CI non-query execution failed: {str(e)}")
            logger.error(f"Query: {query}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()    


    def get_6s_findings(self):
        """Get 6S findings from 5sAudit_measurements table - only pending status"""
        try:
            conn = None
            try:
                conn = get_db_connection_1()  # Use production database for 6S data
                query = '''
                    SELECT measurement_id, audit_id, who, topic_id, details, 
                        until_when, status, pictures
                    FROM dbo.[5sAudit_measurements] 
                    WHERE status = 'Pending' 
                    ORDER BY until_when ASC
                '''
                df = pd.read_sql(query, conn)
                
                findings = []
                for _, row in df.iterrows():
                    # Use 'who' field as the department directly
                    department = str(row.get('who', 'Unknown Department')) if pd.notna(row.get('who')) else 'Unknown Department'
                    
                    # Handle until_when date properly
                    until_when = row.get('until_when')
                    if pd.notna(until_when):
                        if hasattr(until_when, 'isoformat'):
                            until_when_str = until_when.isoformat()
                        else:
                            until_when_str = str(until_when)
                    else:
                        until_when_str = None
                    
                    findings.append({
                        'measurement_id': int(row.get('measurement_id', 0)),
                        'audit_id': str(row.get('audit_id', '')),
                        'who': department,  # This is now the department
                        'topic_id': str(row.get('topic_id', '') if pd.notna(row.get('topic_id')) else ''),
                        'details': str(row.get('details', '') if pd.notna(row.get('details')) else ''),
                        'until_when': until_when_str,
                        'status': str(row.get('status', 'Pending')),
                        'pictures': str(row.get('pictures', '') if pd.notna(row.get('pictures')) else ''),
                        'department': department  # Use 'who' as department
                    })
                
                logger.info(f"Retrieved {len(findings)} pending 6S findings from database")
                return findings
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error getting 6S findings from database: {str(e)}")
            return []



    def mark_6s_finding_complete(self, measurement_id):
        """Mark a 6S finding as complete"""
        try:
            conn = None
            try:
                conn = get_db_connection()  # Use production database
                
                # Update the status to 'Completed' 
                query = '''
                    UPDATE dbo.[5sAudit_measurements] 
                    SET status = 'Completed'
                    WHERE measurement_id = ?
                '''
                
                cursor = conn.cursor()
                cursor.execute(query, (measurement_id,))
                conn.commit()
                rows_affected = cursor.rowcount
                cursor.close()
                
                logger.info(f"Marked 6S finding {measurement_id} as complete, rows affected: {rows_affected}")
                return rows_affected > 0
                
            finally:
                if conn:
                    conn.close()
                    
        except Exception as e:
            logger.error(f"Error marking 6S finding as complete: {str(e)}")
            raise


# Add this method to your ContinuousImprovementDatabase class in app.py

    def upsert_daily_ci_record(self, ci_data):
        """Upsert (insert or update) a daily CI record - only ONE record per date"""
        try:
            # Get the record date
            record_date = ci_data.get('recordDate')
            if record_date:
                try:
                    if 'T' in record_date:
                        date_obj = datetime.fromisoformat(record_date.replace('Z', '+00:00')).date()
                    else:
                        date_obj = datetime.strptime(record_date, '%Y-%m-%d').date()
                except (ValueError, KeyError):
                    date_obj = date.today()
            else:
                date_obj = date.today()
            
            # NO VALIDATION - Accept empty fields
            def safe_int(value, default=0):
                try:
                    if value is None:
                        return default
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            def safe_string(value, default=''):
                if value is None:
                    return default
                return str(value).strip()
            
            meeting_percentage = safe_int(ci_data.get('meetingPercentage'), 100)
            gemba_percentage = safe_int(ci_data.get('gembaPercentage'), 100)
            kamishibai_percentage = safe_int(ci_data.get('kamishibaiPercentage'), 88)
            
            meeting_text = safe_string(ci_data.get('meeting'))
            gemba_text = safe_string(ci_data.get('gemba'))
            kamishibai_text = safe_string(ci_data.get('kamishibai'))
            kamishibai_notes = safe_string(ci_data.get('kamishibaiNotes'))
            created_by = safe_string(ci_data.get('createdBy'), 'System')
            
            # DELETE ALL existing records for this date
            delete_query = """
            DELETE FROM dbo.DailyCI 
            WHERE CAST(RecordDate AS DATE) = CAST(? AS DATE)
            """
            self.execute_non_query(delete_query, params=[date_obj])
            
            # INSERT new record
            insert_query = """
            INSERT INTO dbo.DailyCI (
                Meeting, Gemba, Kamishibai, RecordDate, CreatedDate, CreatedBy,
                MeetingPercentage, GembaPercentage, KamishibaiPercentage, 
                KamishibaiNotes
            ) VALUES (?, ?, ?, ?, GETDATE(), ?, ?, ?, ?, ?)
            """
            
            params = [
                meeting_text,
                gemba_text,
                kamishibai_text,
                date_obj,
                created_by,
                meeting_percentage,
                gemba_percentage,
                kamishibai_percentage,
                kamishibai_notes
            ]
            
            self.execute_non_query(insert_query, params)
            logger.info(f"Replaced CI record for {date_obj}")
            return {'action': 'replaced', 'id': 'new', 'success': True}
            
        except Exception as e:
            logger.error(f"Error in upsert CI record: {str(e)}")
            raise

# Add these methods to your ContinuousImprovementDatabase class in app.py

    def calculate_compass_countdown(self):
        """Calculate days until COMPASS date"""
        try:
            # FIXED: Set COMPASS target date to December 15, 2025
            compass_target_date = date(2025, 12, 15)  # Changed from date(2025, 6, 1)
            today = date.today()
            days_remaining = (compass_target_date - today).days
            return max(0, days_remaining)  # Don't return negative numbers
        except Exception as e:
            logger.error(f"Error calculating COMPASS countdown: {str(e)}")
            return 84  # Default fallback value

    # Also update your get_ci_records method to properly handle the new fields
    def get_ci_records(self):
        """Get all CI records from DailyCI table - Updated to include new fields and proper notes handling"""
        try:
            query = """
            SELECT CIID, Meeting, Gemba, Kamishibai, RecordDate, CreatedDate, CreatedBy,
                MeetingPercentage, GembaPercentage, KamishibaiPercentage, 
                CompassCountdown, KamishibaiNotes, ModifiedBy, ModifiedDate
            FROM dbo.DailyCI 
            ORDER BY RecordDate DESC, CreatedDate DESC
            """
            df = self.execute_query(query)
            
            records = []
            for _, row in df.iterrows():
                # Handle RecordDate properly
                record_date = row.get('RecordDate')
                if pd.notna(record_date):
                    if hasattr(record_date, 'isoformat'):
                        date_str = record_date.isoformat()
                        display_date = record_date.strftime('%Y-%m-%d')
                    else:
                        date_str = str(record_date)
                        display_date = str(record_date)[:10]
                else:
                    date_str = str(date.today())
                    display_date = str(date.today())
                
                def clean_text(text):
                    if pd.isna(text) or text is None:
                        return ''
                    return str(text).strip()
                
                def safe_int(value, default=0):
                    try:
                        return int(value) if pd.notna(value) and value is not None else default
                    except (ValueError, TypeError):
                        return default
                
                # CRITICAL: Properly extract KamishibaiNotes
                kamishibai_notes = clean_text(row.get('KamishibaiNotes'))
                
                records.append({
                    'ciID': str(row.get('CIID', '')),
                    'date': date_str,
                    'displayDate': display_date,
                    'meeting': clean_text(row.get('Meeting')),
                    'gemba': clean_text(row.get('Gemba')),
                    'kamishibai': clean_text(row.get('Kamishibai')),
                    'recordDate': date_str,
                    'createdBy': clean_text(row.get('CreatedBy')),
                    'createdDate': str(row.get('CreatedDate', '')),
                    'modifiedBy': clean_text(row.get('ModifiedBy')),
                    'meetingPercentage': safe_int(row.get('MeetingPercentage'), 100),
                    'gembaPercentage': safe_int(row.get('GembaPercentage'), 100),
                    'kamishibaiPercentage': safe_int(row.get('KamishibaiPercentage'), 88),
                    'compassCountdown': safe_int(row.get('CompassCountdown'), 84),
                    'kamishibaiNotes': kamishibai_notes  # This is the key field
                })
            
            logger.info(f"Retrieved {len(records)} CI records with kamishibaiNotes")
            return records
            
        except Exception as e:
            logger.error(f"Error getting CI records: {str(e)}")
            return []

    def update_daily_ci_record(self, ci_id, ci_data):
        """Update an existing daily CI record"""
        try:
            query = """
            UPDATE dbo.DailyCI SET 
                Meeting = ?, Gemba = ?, Kamishibai = ?, PDCAStatus = ?, 
                RecordDate = ?, ModifiedDate = GETDATE(), ModifiedBy = ?
            WHERE CIID = ?
            """
            
            # Handle record date
            record_date = ci_data.get('recordDate')
            if record_date:
                try:
                    if 'T' in record_date:
                        record_date_obj = datetime.fromisoformat(record_date.replace('Z', '+00:00')).date()
                    else:
                        record_date_obj = datetime.strptime(record_date, '%Y-%m-%d').date()
                except (ValueError, KeyError):
                    record_date_obj = date.today()
            else:
                record_date_obj = date.today()
            
            params = [
                ci_data.get('meeting', ''),
                ci_data.get('gemba', ''),
                ci_data.get('kamishibai', ''),
                ci_data.get('pdcaStatus', ''),
                record_date_obj,
                ci_data.get('modifiedBy', 'System'),
                ci_id
            ]
            
            rows_affected = self.execute_non_query(query, params)
            logger.info(f"Updated daily CI record {ci_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error updating daily CI record: {str(e)}")
            raise

    def delete_daily_ci_record(self, ci_id):
        """Delete a daily CI record"""
        try:
            query = "DELETE FROM dbo.DailyCI WHERE CIID = ?"
            rows_affected = self.execute_non_query(query, [ci_id])
            logger.info(f"Deleted daily CI record {ci_id}, rows affected: {rows_affected}")
            return rows_affected > 0
            
        except Exception as e:
            logger.error(f"Error deleting daily CI record: {str(e)}")
            raise

    def get_daily_ci_record_by_id(self, ci_id):
        """Get a specific daily CI record by ID"""
        try:
            query = "SELECT * FROM dbo.DailyCI WHERE CIID = ?"
            df = self.execute_query(query, params=[ci_id])
            
            if df.empty:
                return None
            
            row = df.iloc[0]
            
            # Convert to expected format
            record = {}
            for key, value in row.items():
                if pd.isna(value):
                    record[key] = None
                elif hasattr(value, 'isoformat'):
                    record[key] = value.isoformat()
                elif isinstance(value, date):
                    record[key] = value.isoformat()
                else:
                    record[key] = str(value) if value is not None else ''
            
            return record
            
        except Exception as e:
            logger.error(f"Error getting daily CI record by ID: {str(e)}")
            raise

# Add these methods to your ContinuousImprovementDatabase class in app.py

    def get_meeting_action_items(self):
        """Get meeting action items from DailyActions table - only active items"""
        try:
            query = '''
                SELECT ActionID, SessionID, SourceType, SourceData, 
                    Department, Assignee, DueDate, Priority, Status, 
                    Notes, CreatedDate, CreatedBy
                FROM dbo.DailyActions 
                WHERE Status != 'Completed' 
                AND (DueDate IS NULL OR DueDate >= GETDATE())
                ORDER BY 
                    CASE Priority 
                        WHEN 'High' THEN 3 
                        WHEN 'Medium' THEN 2 
                        ELSE 1 
                    END DESC, 
                    CreatedDate DESC
            '''
            df = self.execute_query(query)
            
            actions = []
            for _, row in df.iterrows():
                # Parse sourceData JSON
                source_data = {}
                try:
                    if row.get('SourceData'):
                        import json
                        source_data = json.loads(row['SourceData'])
                except:
                    source_data = {'description': 'Action item', 'type': 'Meeting Action'}
                
                # Handle dates properly
                due_date = row.get('DueDate')
                if pd.notna(due_date):
                    if hasattr(due_date, 'isoformat'):
                        due_date_str = due_date.isoformat()
                    else:
                        due_date_str = str(due_date)
                else:
                    due_date_str = None
                
                created_date = row.get('CreatedDate')
                if pd.notna(created_date):
                    if hasattr(created_date, 'isoformat'):
                        created_date_str = created_date.isoformat()
                    else:
                        created_date_str = str(created_date)
                else:
                    created_date_str = str(datetime.now())
                
                actions.append({
                    'actionID': int(row.get('ActionID', 0)),
                    'sessionId': str(row.get('SessionID', '')),
                    'sourceType': str(row.get('SourceType', '')),
                    'sourceData': source_data,
                    'department': str(row.get('Department', '') if pd.notna(row.get('Department')) else ''),
                    'assignee': str(row.get('Assignee', '') if pd.notna(row.get('Assignee')) else ''),
                    'dueDate': due_date_str,
                    'priority': str(row.get('Priority', 'Medium')),
                    'status': str(row.get('Status', 'Pending')),
                    'notes': str(row.get('Notes', '') if pd.notna(row.get('Notes')) else ''),
                    'createdDate': created_date_str,
                    'createdBy': str(row.get('CreatedBy', '') if pd.notna(row.get('CreatedBy')) else ''),
                    'isOverdue': self._is_overdue(due_date_str) if due_date_str else False
                })
            
            logger.info(f"Retrieved {len(actions)} active meeting action items")
            return actions
            
        except Exception as e:
            logger.error(f"Error getting meeting action items: {str(e)}")
            return []

    def _is_overdue(self, due_date_str):
        """Check if an action item is overdue"""
        if not due_date_str:
            return False
        try:
            due_date = datetime.fromisoformat(due_date_str.replace('Z', '+00:00')).date()
            return due_date < date.today()
        except:
            return False

    def cleanup_expired_action_items(self):
        """Clean up action items that are completed or past due by more than 7 days"""
        try:
            # Delete completed items older than 7 days
            cleanup_query = '''
                DELETE FROM dbo.DailyActions 
                WHERE (
                    Status = 'Completed' 
                    AND CreatedDate < DATEADD(day, -7, GETDATE())
                ) OR (
                    DueDate IS NOT NULL 
                    AND DueDate < DATEADD(day, -7, GETDATE())
                    AND Status != 'Completed'
                )
            '''
            
            rows_affected = self.execute_non_query(cleanup_query)
            logger.info(f"Cleaned up {rows_affected} expired meeting action items")
            return rows_affected
            
        except Exception as e:
            logger.error(f"Error cleaning up expired action items: {str(e)}")
            return 0

    def get_meeting_actions_summary(self):
        """Get summary statistics for meeting action items"""
        try:
            summary_query = '''
                SELECT 
                    COUNT(*) as total_actions,
                    SUM(CASE WHEN Status = 'Pending' THEN 1 ELSE 0 END) as pending_actions,
                    SUM(CASE WHEN Status = 'Assigned' THEN 1 ELSE 0 END) as assigned_actions,
                    SUM(CASE WHEN Status = 'In Progress' THEN 1 ELSE 0 END) as in_progress_actions,
                    SUM(CASE WHEN DueDate IS NOT NULL AND DueDate < GETDATE() AND Status != 'Completed' THEN 1 ELSE 0 END) as overdue_actions,
                    SUM(CASE WHEN Priority = 'High' THEN 1 ELSE 0 END) as high_priority_actions
                FROM dbo.DailyActions
                WHERE Status != 'Completed'
            '''
            
            df = self.execute_query(summary_query)
            
            if df.empty:
                return {
                    'total_actions': 0,
                    'pending_actions': 0,
                    'assigned_actions': 0,
                    'in_progress_actions': 0,
                    'overdue_actions': 0,
                    'high_priority_actions': 0
                }
            
            row = df.iloc[0]
            return {
                'total_actions': int(row.get('total_actions', 0)),
                'pending_actions': int(row.get('pending_actions', 0)),
                'assigned_actions': int(row.get('assigned_actions', 0)),
                'in_progress_actions': int(row.get('in_progress_actions', 0)),
                'overdue_actions': int(row.get('overdue_actions', 0)),
                'high_priority_actions': int(row.get('high_priority_actions', 0))
            }
            
        except Exception as e:
            logger.error(f"Error getting meeting actions summary: {str(e)}")
            return {
                'total_actions': 0,
                'pending_actions': 0,
                'assigned_actions': 0,
                'in_progress_actions': 0,
                'overdue_actions': 0,
                'high_priority_actions': 0
            }


    def get_daily_ci_records_by_date_range(self, start_date=None, end_date=None):
        """Get daily CI records within a date range"""
        try:
            if not start_date:
                start_date = date.today() - timedelta(days=30)  # Last 30 days
            if not end_date:
                end_date = date.today()
            
            # FIXED: Include all fields including KamishibaiNotes
            query = """
            SELECT CIID, Meeting, Gemba, Kamishibai, RecordDate, CreatedDate, CreatedBy,
                MeetingPercentage, GembaPercentage, KamishibaiPercentage, 
                CompassCountdown, KamishibaiNotes, ModifiedBy, ModifiedDate
            FROM dbo.DailyCI 
            WHERE RecordDate >= ? AND RecordDate <= ?
            ORDER BY RecordDate DESC, CreatedDate DESC
            """
            df = self.execute_query(query, params=[start_date, end_date])
            
            records = []
            for _, row in df.iterrows():
                # Handle date formatting
                record_date = row.get('RecordDate')
                created_date = row.get('CreatedDate')
                
                if pd.notna(record_date):
                    if hasattr(record_date, 'isoformat'):
                        record_date_str = record_date.isoformat()
                        display_date = record_date.strftime('%Y-%m-%d')
                    else:
                        record_date_str = str(record_date)
                        display_date = str(record_date)[:10]
                else:
                    record_date_str = str(date.today())
                    display_date = str(date.today())
                
                if pd.notna(created_date):
                    if hasattr(created_date, 'isoformat'):
                        created_date_str = created_date.isoformat()
                    else:
                        created_date_str = str(created_date)
                else:
                    created_date_str = str(datetime.now())
                
                # Clean up text fields
                def clean_text(text):
                    if pd.isna(text) or text is None:
                        return ''
                    return str(text).strip()
                
                def safe_int(value, default=0):
                    try:
                        return int(value) if pd.notna(value) and value is not None else default
                    except (ValueError, TypeError):
                        return default
                
                # FIXED: Include all the new fields that the management interface expects
                records.append({
                    'ciID': str(row.get('CIID', '')),
                    'meeting': clean_text(row.get('Meeting')),
                    'gemba': clean_text(row.get('Gemba')),
                    'kamishibai': clean_text(row.get('Kamishibai')),
                    'recordDate': record_date_str,
                    'displayDate': display_date,
                    'createdDate': created_date_str,
                    'createdBy': clean_text(row.get('CreatedBy')),
                    'modifiedBy': clean_text(row.get('ModifiedBy')),
                    # FIXED: Include all percentage fields and notes
                    'meetingPercentage': safe_int(row.get('MeetingPercentage'), 100),
                    'gembaPercentage': safe_int(row.get('GembaPercentage'), 100),
                    'kamishibaiPercentage': safe_int(row.get('KamishibaiPercentage'), 88),
                    'compassCountdown': safe_int(row.get('CompassCountdown'), 84),
                    'kamishibaiNotes': clean_text(row.get('KamishibaiNotes')),  # FIXED: This was missing!
                    # Additional fields for compatibility
                    'pdcaStatus': f"Meeting: {safe_int(row.get('MeetingPercentage'), 100)}%, Gemba: {safe_int(row.get('GembaPercentage'), 100)}%, Kamishibai: {safe_int(row.get('KamishibaiPercentage'), 88)}%, COMPASS: {safe_int(row.get('CompassCountdown'), 84)} days"
                })
            
            logger.info(f"Retrieved {len(records)} CI records by date range with notes field included")
            return records
            
        except Exception as e:
            logger.error(f"Error getting daily CI records by date range: {str(e)}")
            raise

    # Update this existing route in app.py
    @app.route('/api/continuous-improvement/daily-records', methods=['GET', 'POST'])
    def manage_daily_ci_records():
        """API endpoint for managing daily CI records - GET all or POST/UPSERT new"""
        if not ci_db:
            return jsonify({'error': 'CI database not available'}), 503
        
        if request.method == 'GET':
            try:
                # Get date range from query parameters
                start_date = request.args.get('start_date')
                end_date = request.args.get('end_date')
                
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                
                records = ci_db.get_daily_ci_records_by_date_range(start_date, end_date)
                return jsonify(records)
            except Exception as e:
                logger.error(f"API error in get daily CI records: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
        elif request.method == 'POST':
            try:
                ci_data = request.get_json()
                
                # Validate required fields
                # NO VALIDATION - Allow empty fields as per requirements
                # All fields are optional - only recordDate is needed
                if not ci_data.get('recordDate'):
                    return jsonify({'error': 'Missing required field: recordDate'}), 400
                
                # Add created by from session if available
                if session.get('name'):
                    ci_data['createdBy'] = session['name']
                else:
                    ci_data['createdBy'] = 'System'
                
                # Use UPSERT instead of add
                result = ci_db.upsert_daily_ci_record(ci_data)
                
                if result['success']:
                    action_verb = 'updated' if result['action'] == 'updated' else 'created'
                    return jsonify({
                        'success': True, 
                        'message': f'Daily CI record {action_verb} successfully',
                        'action': result['action'],
                        'id': result['id']
                    }), 201
                else:
                    return jsonify({'error': 'Failed to save CI record'}), 500
                    
            except Exception as e:
                logger.error(f"API error in upsert daily CI record: {str(e)}")
                return jsonify({'error': str(e)}), 500

    @app.route('/api/continuous-improvement/daily-records/<ci_id>', methods=['GET', 'PUT', 'DELETE'])
    def manage_single_daily_ci_record(ci_id):
        """API endpoint for managing a single daily CI record"""
        if not ci_db:
            return jsonify({'error': 'CI database not available'}), 503
        
        if request.method == 'GET':
            try:
                record = ci_db.get_daily_ci_record_by_id(ci_id)
                if not record:
                    return jsonify({'error': 'Daily CI record not found'}), 404
                return jsonify(record)
            except Exception as e:
                logger.error(f"API error in get daily CI record: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
        elif request.method == 'PUT':
            try:
                ci_data = request.get_json()
                
                # Add modified by from session if available
                if session.get('name'):
                    ci_data['modifiedBy'] = session['name']
                else:
                    ci_data['modifiedBy'] = 'System'
                
                success = ci_db.update_daily_ci_record(ci_id, ci_data)
                
                if success:
                    return jsonify({
                        'success': True,
                        'message': 'Daily CI record updated successfully'
                    })
                else:
                    return jsonify({'error': 'Daily CI record not found or no changes made'}), 404
                    
            except Exception as e:
                logger.error(f"API error in update daily CI record: {str(e)}")
                return jsonify({'error': str(e)}), 500
        
        elif request.method == 'DELETE':
            try:
                success = ci_db.delete_daily_ci_record(ci_id)
                
                if success:
                    return jsonify({
                        'success': True,
                        'message': 'Daily CI record deleted successfully'
                    })
                else:
                    return jsonify({'error': 'Daily CI record not found'}), 404
                    
            except Exception as e:
                logger.error(f"API error in delete daily CI record: {str(e)}")
                return jsonify({'error': str(e)}), 500
            
    def get_improvement_summary(self):
        """Get continuous improvement summary from DailyCI table and PDCA cards"""
        try:
            summary = {}
            
            # Get total CI entries
            total_query = """
            SELECT COUNT(*) as count FROM dbo.DailyCI
            """
            total_df = self.execute_query(total_query)
            summary['total_entries'] = int(total_df.iloc[0]['count']) if not total_df.empty else 0
            
            # Get entries from current month
            monthly_query = """
            SELECT COUNT(*) as count FROM dbo.DailyCI
            WHERE MONTH(RecordDate) = MONTH(GETDATE()) AND YEAR(RecordDate) = YEAR(GETDATE())
            """
            monthly_df = self.execute_query(monthly_query)
            summary['monthly_entries'] = int(monthly_df.iloc[0]['count']) if not monthly_df.empty else 0
            
            # Get recent entries (last 7 days)
            recent_query = """
            SELECT COUNT(*) as count FROM dbo.DailyCI
            WHERE RecordDate >= DATEADD(day, -7, GETDATE())
            """
            recent_df = self.execute_query(recent_query)
            summary['recent_entries'] = int(recent_df.iloc[0]['count']) if not recent_df.empty else 0
            
            # Get PDCA card counts
            pdca_total_query = """
            SELECT COUNT(*) as count FROM dbo.DailyPDCACards
            """
            pdca_total_df = self.execute_query(pdca_total_query)
            total_pdca = int(pdca_total_df.iloc[0]['count']) if not pdca_total_df.empty else 0
            
            # Get overdue PDCA cards
            pdca_overdue_query = """
            SELECT COUNT(*) as count FROM dbo.DailyPDCACards 
            WHERE date < DATEADD(day, -30, GETDATE()) OR date IS NULL
            """
            pdca_overdue_df = self.execute_query(pdca_overdue_query)
            overdue_pdca = int(pdca_overdue_df.iloc[0]['count']) if not pdca_overdue_df.empty else 0
            
            # Calculate derived metrics
            summary['active_projects'] = max(1, summary['recent_entries'] + total_pdca)
            summary['completed_initiatives'] = max(0, summary['total_entries'] - summary['recent_entries'])
            summary['cost_savings'] = (summary['total_entries'] + total_pdca) * 1250.0  # Mock: $1,250 per entry
            summary['efficiency_gains'] = min(25.0, (summary['monthly_entries'] + total_pdca) * 2.5)  # Mock calculation
            summary['employee_suggestions'] = summary['total_entries'] + total_pdca
            summary['ci_records'] = summary['total_entries']
            summary['overdue_pdca'] = overdue_pdca
            
            logger.info(f"Continuous improvement summary generated: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating CI summary: {str(e)}")
            # Return default values on error
            return {
                'active_projects': 0,
                'completed_initiatives': 0,
                'cost_savings': 0,
                'efficiency_gains': 0,
                'employee_suggestions': 0,
                'ci_records': 0,
                'overdue_pdca': 0,
                'total_entries': 0,
                'monthly_entries': 0,
                'recent_entries': 0
            }

    # PDCA Card methods moved from Quality to CI

    def get_pdca_cards(self):
        """Get PDCA cards from DailyPDCACards table"""
        try:
            query = """
            SELECT * FROM dbo.DailyPDCACards 
            ORDER BY pdcaID DESC
            """
            df = self.execute_query(query)
            
            cards = []
            for _, row in df.iterrows():
                # Determine status based on escalation and next steps
                status = 'New'
                escalated_value = row.get('escalated')
                if pd.notna(escalated_value) and str(escalated_value).strip():
                    status = 'Escalated'
                elif pd.notna(row.get('date')):
                    status = 'In Progress'  # If date is set, it's in progress
                elif pd.notna(row.get('nextSteps')) and 'complete' in str(row.get('nextSteps')).lower():
                    status = 'Completed'
                elif pd.notna(row.get('nextSteps')):
                    status = 'In Progress'
                
                # Handle date field properly
                date_value = row.get('date')
                if pd.notna(date_value):
                    if hasattr(date_value, 'isoformat'):
                        created_date = date_value.isoformat()
                        date_str = date_value.isoformat()
                    else:
                        created_date = str(date_value)
                        date_str = str(date_value)
                else:
                    created_date = 'N/A'
                    date_str = ''
                
                cards.append({
                    'pdcaID': str(row.get('pdcaID', '')),
                    'createdDate': created_date,
                    'title': str(row.get('taskDetails', '') if pd.notna(row.get('taskDetails')) else ''),
                    'taskDetails': str(row.get('taskDetails', '') if pd.notna(row.get('taskDetails')) else ''),
                    'department': str(row.get('station', '') if pd.notna(row.get('station')) else ''),
                    'station': str(row.get('station', '') if pd.notna(row.get('station')) else ''),
                    'responsible': str(row.get('responsible', '') if pd.notna(row.get('responsible')) else ''),
                    'escalated': str(escalated_value if pd.notna(escalated_value) else ''),
                    'escalatedTo': str(escalated_value if pd.notna(escalated_value) else ''),  # Keep for backward compatibility
                    'date': date_str,
                    'nextSteps': str(row.get('nextSteps', '') if pd.notna(row.get('nextSteps')) else ''),
                    'status': status,
                    'assignedTo': str(row.get('responsible', '') if pd.notna(row.get('responsible')) else '')
                })
            
            return cards
            
        except Exception as e:
            logger.error(f"Error getting PDCA cards: {str(e)}")
            raise

    def add_pdca_card(self, pdca_data):
        """Add a new PDCA card"""
        try:
            # Validate required fields - allow either 'title' or 'taskDetails'
            task_details = pdca_data.get('taskDetails') or pdca_data.get('title', '')
            if not task_details:
                raise ValueError('Missing required field: taskDetails or title')
            
            # Handle date field conversion - FIXED
            date_value = None
            date_field = pdca_data.get('date') or pdca_data.get('createdDate')
            if date_field:
                try:
                    if isinstance(date_field, str):
                        if 'T' in date_field:
                            date_value = datetime.fromisoformat(date_field.replace('Z', '+00:00'))
                        else:
                            # Try different datetime formats
                            try:
                                date_value = datetime.strptime(date_field, '%Y-%m-%dT%H:%M')
                            except ValueError:
                                try:
                                    date_value = datetime.strptime(date_field, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    date_value = datetime.strptime(date_field, '%Y-%m-%d')
                    elif isinstance(date_field, datetime):
                        date_value = date_field
                    else:
                        logger.warning(f"Unexpected date field type: {type(date_field)}")
                        date_value = None
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format for date: {str(ve)}")
                    date_value = None
            
            # Get station from either 'station' or 'department'
            station = pdca_data.get('station') or pdca_data.get('department', '')
            
            # Get escalated from either 'escalated' or 'escalatedTo'
            escalated = pdca_data.get('escalated') or pdca_data.get('escalatedTo', '')
            
            # Insert into database
            query = """
            INSERT INTO dbo.DailyPDCACards (
                taskDetails, station, responsible, escalated, date, nextSteps
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
            
            params = [
                task_details,
                station,
                pdca_data.get('responsible', ''),
                escalated,
                date_value,  # This is now properly handled
                pdca_data.get('nextSteps', '')
            ]
            
            rows_affected = self.execute_non_query(query, params)
            logger.info(f"Added PDCA card, rows affected: {rows_affected}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding PDCA card: {str(e)}")
            raise

    def update_pdca_card(self, pdca_id, pdca_data):
        """Update an existing PDCA card"""
        try:
            # Handle date field conversion - FIXED
            date_value = None
            date_field = pdca_data.get('date') or pdca_data.get('createdDate')
            if date_field:
                try:
                    if isinstance(date_field, str):
                        if 'T' in date_field:
                            date_value = datetime.fromisoformat(date_field.replace('Z', '+00:00'))
                        else:
                            # Try different datetime formats
                            try:
                                date_value = datetime.strptime(date_field, '%Y-%m-%dT%H:%M')
                            except ValueError:
                                try:
                                    date_value = datetime.strptime(date_field, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    date_value = datetime.strptime(date_field, '%Y-%m-%d')
                    elif isinstance(date_field, datetime):
                        date_value = date_field
                    else:
                        logger.warning(f"Unexpected date field type: {type(date_field)}")
                        date_value = None
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format for date: {str(ve)}")
                    date_value = None
            
            # Get fields from either possible name
            task_details = pdca_data.get('taskDetails') or pdca_data.get('title', '')
            station = pdca_data.get('station') or pdca_data.get('department', '')
            escalated = pdca_data.get('escalated') or pdca_data.get('escalatedTo', '')
            
            # Update using correct column names
            query = """
            UPDATE dbo.DailyPDCACards SET 
                taskDetails = ?, station = ?, responsible = ?, escalated = ?, 
                date = ?, nextSteps = ?
            WHERE pdcaID = ?
            """
            
            params = [
                task_details,
                station,
                pdca_data.get('responsible', ''),
                escalated,
                date_value,  # This is now properly handled
                pdca_data.get('nextSteps', ''),
                pdca_id
            ]
            
            rows_affected = self.execute_non_query(query, params)
            
            if rows_affected > 0:
                logger.info(f"Updated PDCA card {pdca_id}, rows affected: {rows_affected}")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error updating PDCA card: {str(e)}")
            raise


    def delete_pdca_card(self, pdca_id):
        """Delete a PDCA card"""
        try:
            query = "DELETE FROM dbo.DailyPDCACards WHERE pdcaID = ?"
            rows_affected = self.execute_non_query(query, [pdca_id])
            
            if rows_affected > 0:
                logger.info(f"Deleted PDCA card {pdca_id}, rows affected: {rows_affected}")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error deleting PDCA card: {str(e)}")
            raise

    def get_pdca_card_by_id(self, pdca_id):
        """Get a specific PDCA card by ID"""
        try:
            query = "SELECT * FROM dbo.DailyPDCACards WHERE pdcaID = ?"
            df = self.execute_query(query, params=[pdca_id])
            
            if df.empty:
                return None
            
            row = df.iloc[0]
            
            # Convert to expected format with ALL necessary fields
            pdca_card = {
                'pdcaID': str(row.get('pdcaID', '')),
                'title': str(row.get('taskDetails', '')),
                'taskDetails': str(row.get('taskDetails', '')),
                'department': str(row.get('station', '')),
                'station': str(row.get('station', '')),
                'responsible': str(row.get('responsible', '')),
                'escalated': str(row.get('escalated', '')),
                'escalatedTo': str(row.get('escalated', '')),  # For backward compatibility
                'date': str(row.get('date', '')) if pd.notna(row.get('date')) else '',
                'createdDate': str(row.get('date', '')) if pd.notna(row.get('date')) else 'N/A',
                'nextSteps': str(row.get('nextSteps', '')),
                'status': 'New',  # Default status
                'assignedTo': str(row.get('responsible', ''))
            }
            
            # Convert datetime to proper format if it exists
            if pdca_card['date'] and pdca_card['date'] != '':
                try:
                    if hasattr(row['date'], 'isoformat'):
                        pdca_card['date'] = row['date'].isoformat()
                        pdca_card['createdDate'] = row['date'].isoformat()
                except:
                    pass  # Keep as string if conversion fails
            
            return pdca_card
            
        except Exception as e:
            logger.error(f"Error getting PDCA card by ID: {str(e)}")
            raise

    def get_improvement_summary(self):
        """Get continuous improvement summary from DailyCI table"""
        try:
            summary = {}
            
            # Get total CI entries
            total_query = """
            SELECT COUNT(*) as count FROM dbo.DailyCI
            """
            total_df = self.execute_query(total_query)
            summary['total_entries'] = int(total_df.iloc[0]['count']) if not total_df.empty else 0
            
            # Get entries from current month
            monthly_query = """
            SELECT COUNT(*) as count FROM dbo.DailyCI
            WHERE MONTH(CIID) = MONTH(GETDATE()) AND YEAR(CIID) = YEAR(GETDATE())
            """
            monthly_df = self.execute_query(monthly_query)
            summary['monthly_entries'] = int(monthly_df.iloc[0]['count']) if not monthly_df.empty else 0
            
            # Get recent entries (last 7 days) - using CIID as timestamp
            recent_query = """
            SELECT COUNT(*) as count FROM dbo.DailyCI
            WHERE CIID >= DATEADD(day, -7, GETDATE())
            """
            recent_df = self.execute_query(recent_query)
            summary['recent_entries'] = int(recent_df.iloc[0]['count']) if not recent_df.empty else 0
            
            # Calculate derived metrics
            summary['active_projects'] = max(1, summary['recent_entries'])
            summary['completed_initiatives'] = max(0, summary['total_entries'] - summary['recent_entries'])
            summary['cost_savings'] = summary['total_entries'] * 1250.0  # Mock: $1,250 per entry
            summary['efficiency_gains'] = min(25.0, summary['monthly_entries'] * 2.5)  # Mock calculation
            summary['employee_suggestions'] = summary['total_entries']
            
            logger.info(f"Continuous improvement summary generated: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating CI summary: {str(e)}")
            # Return default values on error
            return {
                'active_projects': 0,
                'completed_initiatives': 0,
                'cost_savings': 0,
                'efficiency_gains': 0,
                'employee_suggestions': 0,
                'total_entries': 0,
                'monthly_entries': 0,
                'recent_entries': 0
            }
        




# Initialize the new database connections
try:
    logistics_db = LogisticsDatabase()
    logger.info("Logistics database initialized successfully")
except Exception as e:
    logger.critical(f"Failed to initialize logistics database: {str(e)}")
    logistics_db = None

try:
    ci_db = ContinuousImprovementDatabase()
    logger.info("Continuous improvement database initialized successfully")
except Exception as e:
    logger.critical(f"Failed to initialize CI database: {str(e)}")
    ci_db = None


# Add these page routes after your existing routes

# Add these methods to your existing ContinuousImprovementDatabase class

# Employee Suggestions methods (add to ContinuousImprovementDatabase class)



# Add these methods to your existing ContinuousImprovementDatabase class




# Update the get_improvement_summary method to include suggestions count
def get_improvement_summary(self):
    """Get continuous improvement summary from DailyCI table and other sources"""
    try:
        summary = {}
        
        # Get CI records count
        ci_query = """
        SELECT COUNT(*) as count FROM dbo.DailyCI
        WHERE RecordDate >= DATEADD(day, -30, GETDATE())
        """
        ci_df = self.execute_query(ci_query)
        summary['ci_records'] = int(ci_df.iloc[0]['count']) if not ci_df.empty else 0
        
        # Get PDCA cards count
        pdca_query = """
        SELECT COUNT(*) as count FROM dbo.DailyPDCACards
        """
        pdca_df = self.execute_query(pdca_query)
        summary['pdca_cards'] = int(pdca_df.iloc[0]['count']) if not pdca_df.empty else 0
        
        # Get employee suggestions count
        suggestions_query = """
        SELECT COUNT(*) as count FROM dbo.EmployeeSuggestions
        WHERE CreatedDate >= DATEADD(day, -30, GETDATE())
        """
        suggestions_df = self.execute_query(suggestions_query)
        summary['employee_suggestions'] = int(suggestions_df.iloc[0]['count']) if not suggestions_df.empty else 0
        
        # Get 6S findings count
        findings = self.get_6s_findings()
        summary['s6_findings'] = len(findings)
        
        # Set other summary fields to 0 (removed cards)
        summary['active_projects'] = 0
        summary['completed_initiatives'] = 0
        summary['cost_savings'] = 0
        summary['efficiency_gains'] = 0
        summary['overdue_pdca'] = 0
        
        logger.info(f"Continuous improvement summary generated: {summary}")
        return summary
        
    except Exception as e:
        logger.error(f"Error generating CI summary: {str(e)}")
        # Return default values on error
        return {
            'employee_suggestions': 0,
            'ci_records': 0,
            'pdca_cards': 0,
            's6_findings': 0,
            'active_projects': 0,
            'completed_initiatives': 0,
            'cost_savings': 0,
            'efficiency_gains': 0,
            'overdue_pdca': 0
        }

# Remove all employee suggestions API routes since that card was removed

# Keep only the 6S findings API routes
# Add these methods to the ContinuousImprovementDatabase class
@app.route('/api/continuous-improvement/6s-findings', methods=['GET'])
def get_6s_findings_api():
    """API endpoint for 6S findings"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        findings = ci_db.get_6s_findings()
        return jsonify(findings)
    except Exception as e:
        logger.error(f"API error in get 6S findings: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/continuous-improvement/6s-findings/<int:measurement_id>/complete', methods=['POST'])
def mark_6s_finding_complete_api(measurement_id):
    """API endpoint to mark 6S finding as complete"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        success = ci_db.mark_6s_finding_complete(measurement_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': '6S finding marked as complete successfully'
            })
        else:
            return jsonify({'error': '6S finding not found'}), 404
            
    except Exception as e:
        logger.error(f"API error in mark 6S finding complete: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Remove the /measures route - 6S findings will be displayed in CI dashboard presentation mode




@app.route('/logistics')
def logistics_dashboard_page():
    """Logistics dashboard page"""
    return render_template('logistics.html')

@app.route('/logistics-management')
@require_logistics_access
def logistics_management_page():
    """Logistics management page"""
    return render_template('logistics_management.html')

@app.route('/ci')
def continuous_improvement_dashboard_page():
    """Continuous improvement dashboard page"""
    return render_template('ci.html')

@app.route('/ci-management')
@require_ci_access
def continuous_improvement_management_page():
    """Continuous improvement management page"""
    return render_template('ci_management.html')


# Logistics API Routes

@app.route('/api/logistics/downtime-selection', methods=['POST'])
def update_logistics_downtime_selections():
    """API endpoint to update which downtime events to display"""
    if not logistics_db:
        return jsonify({'error': 'Logistics database not available'}), 503
    
    try:
        data = request.get_json()
        selected_ids = data.get('selectedIds', [])
        
        # Get user from session
        user = session.get('name', 'System')
        
        # Use the LogisticsDatabase method to save selections
        success = logistics_db.save_downtime_selection(selected_ids, user)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Updated selection: {len(selected_ids)} events selected'
            })
        else:
            return jsonify({'error': 'Failed to save selection'}), 500
            
    except Exception as e:
        logger.error(f"API error in update_logistics_downtime_selections: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/logistics/summary')
def logistics_summary():
    """API endpoint for logistics summary"""
    if not logistics_db:
        return jsonify({'error': 'Logistics database not available'}), 503
    
    try:
        summary = logistics_db.get_logistics_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in logistics_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/logistics/material-downtime')
def logistics_material_downtime():
    """API endpoint for material downtime events"""
    if not logistics_db:
        return jsonify({'error': 'Logistics database not available'}), 503
    
    try:
        days = request.args.get('days', 1, type=int)
        events = logistics_db.get_material_downtime(days)
        return jsonify(events)
    except Exception as e:
        logger.error(f"API error in material_downtime: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/logistics/suppliers', methods=['GET', 'POST'])
def logistics_suppliers():
    """API endpoint for suppliers from DailySuppliers table"""
    if not logistics_db:
        return jsonify({'error': 'Logistics database not available'}), 503
    
    if request.method == 'GET':
        try:
            suppliers = logistics_db.get_suppliers()
            return jsonify(suppliers)
        except Exception as e:
            logger.error(f"API error in get suppliers: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'POST':
        try:
            supplier_data = request.get_json()
            
            # Validate required fields
            required_fields = ['number']
            for field in required_fields:
                if not supplier_data.get(field):
                    return jsonify({'error': f'Missing required field: {field}'}), 400
            
            success = logistics_db.add_supplier(supplier_data)
            
            return jsonify({
                'success': True, 
                'message': 'Supplier added successfully'
            }), 201
            
        except Exception as e:
            logger.error(f"API error in add supplier: {str(e)}")
            return jsonify({'error': str(e)}), 500

@app.route('/api/logistics/suppliers/<supplier_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_supplier(supplier_id):
    """API endpoint for managing a single supplier"""
    if not logistics_db:
        return jsonify({'error': 'Logistics database not available'}), 503
    
    if request.method == 'GET':
        try:
            supplier = logistics_db.get_supplier_by_id(supplier_id)
            if not supplier:
                return jsonify({'error': 'Supplier not found'}), 404
            return jsonify(supplier)
        except Exception as e:
            logger.error(f"API error in get supplier: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            supplier_data = request.get_json()
            
            success = logistics_db.update_supplier(supplier_id, supplier_data)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Supplier updated successfully'
                })
            else:
                return jsonify({'error': 'Supplier not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update supplier: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            success = logistics_db.delete_supplier(supplier_id)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Supplier deleted successfully'
                })
            else:
                return jsonify({'error': 'Supplier not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete supplier: {str(e)}")
            return jsonify({'error': str(e)}), 500

# Placeholder endpoints for frontend compatibility (return empty arrays)
@app.route('/api/logistics/shipments', methods=['GET', 'POST'])
def logistics_shipments():
    """Placeholder endpoint for shipments"""
    if request.method == 'GET':
        return jsonify([])
    else:
        return jsonify({'success': True, 'message': 'Shipments not implemented'}), 201


@app.route('/api/logistics/inventory', methods=['GET', 'POST'])
def logistics_inventory():
    """Placeholder endpoint for inventory"""
    if request.method == 'GET':
        return jsonify([])
    else:
        return jsonify({'success': True, 'message': 'Inventory not implemented'}), 201

@app.route('/api/logistics/inventory/<item_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_inventory_item(item_id):
    """Placeholder endpoint for single inventory item"""
    if request.method == 'GET':
        return jsonify({'error': 'Item not found'}), 404
    elif request.method == 'PUT':
        return jsonify({'success': True, 'message': 'Inventory not implemented'})
    else:
        return jsonify({'success': True, 'message': 'Inventory not implemented'})


@app.route('/api/meeting-actions/active', methods=['GET'])
def get_active_meeting_actions():
    """API endpoint for active meeting action items"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        actions = ci_db.get_meeting_action_items()
        return jsonify(actions)
    except Exception as e:
        logger.error(f"API error in get active meeting actions: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/meeting-actions/cleanup', methods=['POST'])
def cleanup_meeting_actions():
    """API endpoint to clean up expired meeting action items"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        rows_cleaned = ci_db.cleanup_expired_action_items()
        return jsonify({
            'success': True,
            'message': f'Cleaned up {rows_cleaned} expired action items'
        })
    except Exception as e:
        logger.error(f"API error in cleanup meeting actions: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/meeting-actions/summary', methods=['GET'])
def get_meeting_actions_summary():
    """API endpoint for meeting actions summary statistics"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        summary = ci_db.get_meeting_actions_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in get meeting actions summary: {str(e)}")
        return jsonify({'error': str(e)}), 500
    



# Update existing meeting action item by ID
@app.route('/api/meeting-actions/<int:action_id>', methods=['PUT'])
def update_meeting_action(action_id):
    """API endpoint to update a meeting action item"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        action_data = request.get_json()
        
        # Handle due date conversion
        due_date = None
        if action_data.get('dueDate'):
            try:
                due_date = datetime.fromisoformat(action_data['dueDate'].replace('Z', '+00:00'))
            except:
                due_date = None
        
        query = """
        UPDATE dbo.DailyActions SET 
            Department = ?, Assignee = ?, DueDate = ?, Priority = ?, 
            Status = ?, Notes = ?
        WHERE ActionID = ?
        """
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        cursor.execute(query, [
            action_data.get('department', ''),
            action_data.get('assignee', ''),
            due_date,
            action_data.get('priority', 'Medium'),
            action_data.get('status', 'Pending'),
            action_data.get('notes', ''),
            action_id
        ])
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_affected > 0:
            return jsonify({
                'success': True,
                'message': 'Action item updated successfully'
            })
        else:
            return jsonify({'error': 'Action item not found'}), 404
            
    except Exception as e:
        logger.error(f"Error updating meeting action: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/meeting-actions/<int:action_id>', methods=['DELETE'])
def delete_meeting_action(action_id):
    """API endpoint to delete a meeting action item"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        query = """
        DELETE FROM dbo.DailyActions 
        WHERE ActionID = ?
        """
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        cursor.execute(query, [action_id])
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_affected > 0:
            return jsonify({
                'success': True,
                'message': 'Action item deleted successfully'
            })
        else:
            return jsonify({'error': 'Action item not found'}), 404
            
    except Exception as e:
        logger.error(f"Error deleting meeting action: {str(e)}")
        return jsonify({'error': str(e)}), 500


# Complete a meeting action item
@app.route('/api/meeting-actions/<int:action_id>/complete', methods=['POST'])
def complete_meeting_action(action_id):
    """API endpoint to mark a meeting action item as complete"""
    if not session.get('logged_in'):
        return jsonify({'error': 'Unauthorized'}), 401
    
    try:
        query = """
        UPDATE dbo.DailyActions SET 
            Status = 'Completed', 
            CompletedDate = GETDATE(),
            CompletedBy = ?
        WHERE ActionID = ?
        """
        
        conn = get_db_connection_1()
        cursor = conn.cursor()
        cursor.execute(query, [
            session.get('name', 'System'),
            action_id
        ])
        rows_affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if rows_affected > 0:
            return jsonify({
                'success': True,
                'message': 'Action item marked as complete'
            })
        else:
            return jsonify({'error': 'Action item not found'}), 404
            
    except Exception as e:
        logger.error(f"Error completing meeting action: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/continuous-improvement/summary')
def continuous_improvement_summary():
    """API endpoint for continuous improvement summary - Updated to include meeting actions"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        summary = ci_db.get_improvement_summary()
        
        # Add meeting actions count to summary
        try:
            meeting_actions = ci_db.get_meeting_action_items()
            summary['meeting_actions'] = len(meeting_actions)
            
            # Add meeting actions summary stats
            meeting_summary = ci_db.get_meeting_actions_summary()
            summary['meeting_actions_overdue'] = meeting_summary.get('overdue_actions', 0)
            summary['meeting_actions_high_priority'] = meeting_summary.get('high_priority_actions', 0)
        except Exception as e:
            logger.warning(f"Could not load meeting actions for summary: {str(e)}")
            summary['meeting_actions'] = 0
            summary['meeting_actions_overdue'] = 0
            summary['meeting_actions_high_priority'] = 0
        
        return jsonify(summary)
    except Exception as e:
        logger.error(f"API error in ci_summary: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

@app.route('/api/continuous-improvement/records')
def continuous_improvement_records():
    """API endpoint for CI records from DailyCI table"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    try:
        records = ci_db.get_ci_records()
        return jsonify(records)
    except Exception as e:
        logger.error(f"API error in ci_records: {str(e)}")
        return jsonify({'error': str(e)}), 500

# For compatibility with the management HTML, provide empty endpoints for projects, kaizen, suggestions, metrics
@app.route('/api/continuous-improvement/projects')
def continuous_improvement_projects():
    """API endpoint for projects (placeholder)"""
    return jsonify([])

@app.route('/api/continuous-improvement/pdca-cards', methods=['GET', 'POST'])
def ci_pdca_cards():
    """API endpoint for PDCA cards - GET all or POST new"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    if request.method == 'GET':
        try:
            cards = ci_db.get_pdca_cards()
            return jsonify(cards)
        except Exception as e:
            logger.error(f"API error in get PDCA cards: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'POST':
        try:
            pdca_data = request.get_json()
            
            # Validate required fields - allow either 'title' or 'taskDetails'
            task_details = pdca_data.get('taskDetails') or pdca_data.get('title', '')
            if not task_details:
                return jsonify({'error': 'Missing required field: taskDetails or title'}), 400
            
            # Handle date field conversion
            date_field = pdca_data.get('date') or pdca_data.get('createdDate')
            if date_field:
                try:
                    if 'T' in date_field:
                        date_value = datetime.fromisoformat(date_field.replace('Z', '+00:00'))
                    else:
                        date_value = datetime.strptime(date_field, '%Y-%m-%d %H:%M:%S')
                    pdca_data['date'] = date_value
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format for date: {str(ve)}")
                    return jsonify({'error': 'Invalid datetime format for date'}), 400
            
            success = ci_db.add_pdca_card(pdca_data)
            
            return jsonify({
                'success': True, 
                'message': 'PDCA card added successfully'
            }), 201
            
        except Exception as e:
            logger.error(f"API error in add PDCA card: {str(e)}")
            return jsonify({'error': str(e)}), 500
        
@app.route('/api/continuous-improvement/pdca-cards/<pdca_id>', methods=['GET', 'PUT', 'DELETE'])
def manage_single_ci_pdca_card(pdca_id):
    """API endpoint for managing a single PDCA card in CI"""
    if not ci_db:
        return jsonify({'error': 'CI database not available'}), 503
    
    if request.method == 'GET':
        try:
            card = ci_db.get_pdca_card_by_id(pdca_id)
            if not card:
                return jsonify({'error': 'PDCA card not found'}), 404
            return jsonify(card)
        except Exception as e:
            logger.error(f"API error in get PDCA card: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        try:
            pdca_data = request.get_json()
            
            # Handle date field conversion
            date_field = pdca_data.get('date') or pdca_data.get('createdDate')
            if date_field:
                try:
                    if 'T' in date_field:
                        date_value = datetime.fromisoformat(date_field.replace('Z', '+00:00'))
                    else:
                        date_value = datetime.strptime(date_field, '%Y-%m-%d %H:%M:%S')
                    pdca_data['date'] = date_value
                except (ValueError, KeyError) as ve:
                    logger.error(f"Invalid datetime format for date: {str(ve)}")
                    return jsonify({'error': 'Invalid datetime format for date'}), 400
            
            success = ci_db.update_pdca_card(pdca_id, pdca_data)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'PDCA card updated successfully'
                })
            else:
                return jsonify({'error': 'PDCA card not found or no changes made'}), 404
                
        except Exception as e:
            logger.error(f"API error in update PDCA card: {str(e)}")
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'DELETE':
        try:
            success = ci_db.delete_pdca_card(pdca_id)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'PDCA card deleted successfully'
                })
            else:
                return jsonify({'error': 'PDCA card not found'}), 404
                
        except Exception as e:
            logger.error(f"API error in delete PDCA card: {str(e)}")
            return jsonify({'error': str(e)}), 500

# Daily CI Records API Routes




@app.route('/api/continuous-improvement/kaizen-events')
def continuous_improvement_kaizen():
    """API endpoint for kaizen events (placeholder)"""
    return jsonify([])

@app.route('/api/continuous-improvement/suggestions')
def continuous_improvement_suggestions():
    """API endpoint for suggestions (placeholder)"""
    return jsonify([])

@app.route('/api/continuous-improvement/metrics')
def continuous_improvement_metrics():
    """API endpoint for metrics (placeholder)"""
    return jsonify([])


# Health check endpoints
@app.route('/api/logistics/health')
def logistics_health_check():
    """Logistics system health check endpoint"""
    try:
        if logistics_db:
            conn = logistics_db.get_connection()
            conn.close()
            return jsonify({'status': 'healthy', 'logistics_database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'logistics_database': 'disconnected'}), 503
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

@app.route('/api/continuous-improvement/health')
def continuous_improvement_health_check():
    """CI system health check endpoint"""
    try:
        if ci_db:
            conn = ci_db.get_connection()
            conn.close()
            return jsonify({'status': 'healthy', 'ci_database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'ci_database': 'disconnected'}), 503
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

# Login and Authentication
@app.route("/login", methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form.get("username")
        password = request.form.get("password")
        user_data = searchUser(username, password)
        
        if user_data:
            session["name"] = user_data[2]
            session["userID"] = user_data[0]
            session["logged_in"] = True
            session["email"] = user_data[5]
            return redirect(url_for('dashboard'))
        else:    
            return render_template('login.html', error='User or Password Invalid, Please try again!')
    else:
        return render_template('login.html')
    
def searchUser(username, password):
    """Search for user in database"""
    conn = get_db_connection()
    if not conn:
        return None
        
    try:
        cursor = conn.cursor()
        query = """
            SELECT * 
            FROM Users 
            WHERE login = ? COLLATE SQL_Latin1_General_CP1_CS_AS 
            AND password = ? COLLATE SQL_Latin1_General_CP1_CS_AS
        """
        userDetail = cursor.execute(query, (username, password)).fetchone()
        return userDetail
    except Exception as e:
        logger.error(f"Error in searchUser: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

try:
    production_db = ProductionDatabase()
    logger.info("Production database initialized successfully")
except Exception as e:
    logger.critical(f"Failed to initialize production database: {str(e)}")
    production_db = None


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'error': 'Bad request'}), 400

from flask import Flask, render_template, session, redirect, url_for, request, jsonify
from datetime import datetime
import logging
import os

# Set up logging
logger = logging.getLogger(__name__)

# PDF Output folder
PDF_OUTPUT_FOLDER = r'U:\Production\Daily OpEX Report'

# REPLACE your comprehensive_report() route with this fixed version

@app.route('/report/comprehensive')
def comprehensive_report():
    """Generate comprehensive HTML report (user can print to PDF from browser)"""
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    try:
        # Gather all data
        report_data = {
            'generated_date': datetime.now().strftime('%B %d, %Y at %I:%M %p'),
            'generated_by': session.get('name', 'System'),
            'safety_summary': None,
            'quality_summary': None,
            'production_summary': None,
            'logistics_summary': None,
            'ci_summary': None,
            'safety_incidents': [],
            'quality_alerts': [],
            'downtime_events': [],
            'material_downtime': [],
            'ci_records': [],
            'pdca_cards': [],
            's6_findings': [],
            'meeting_actions': []
        }
        
        # Fetch summary data with proper error handling
        try:
            if safety_db:
                report_data['safety_summary'] = safety_db.get_safety_summary()
        except Exception as e:
            logger.error(f"Error fetching safety summary: {str(e)}")
        
        try:
            if quality_db:
                report_data['quality_summary'] = quality_db.get_quality_summary()
        except Exception as e:
            logger.error(f"Error fetching quality summary: {str(e)}")
        
        try:
            if production_db:
                report_data['production_summary'] = production_db.get_daily_production_summary()
        except Exception as e:
            logger.error(f"Error fetching production summary: {str(e)}")
        
        try:
            if logistics_db:
                report_data['logistics_summary'] = logistics_db.get_logistics_summary()
        except Exception as e:
            logger.error(f"Error fetching logistics summary: {str(e)}")
        
        # === CRITICAL FIX: Load CI data FIRST, then calculate summary ===
        
        # 1. Fetch PDCA Cards - using your existing method
        try:
            if ci_db:
                pdca_cards = ci_db.get_pdca_cards()
                if pdca_cards:
                    report_data['pdca_cards'] = pdca_cards
                logger.info(f"Loaded {len(report_data['pdca_cards'])} PDCA cards")
        except Exception as e:
            logger.error(f"Error fetching PDCA cards: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # 2. Fetch 6S Findings - using your existing method
        try:
            if ci_db:
                s6_findings = ci_db.get_6s_findings()
                if s6_findings:
                    report_data['s6_findings'] = s6_findings
                logger.info(f"Loaded {len(report_data['s6_findings'])} 6S findings")
        except Exception as e:
            logger.error(f"Error fetching 6S findings: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # 3. Fetch CI Records - using your existing method
        try:
            if ci_db:
                ci_records = ci_db.get_ci_records()
                if ci_records:
                    report_data['ci_records'] = ci_records[:5]
                logger.info(f"Loaded {len(report_data['ci_records'])} CI records")
        except Exception as e:
            logger.error(f"Error fetching CI records: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # 4. OPTIONAL: Fetch Meeting Actions if you have them
        try:
            # If you have a meeting_actions_db or similar
            # meeting_actions = meeting_actions_db.get_active_actions()
            # report_data['meeting_actions'] = meeting_actions
            pass
        except Exception as e:
            logger.error(f"Error fetching meeting actions: {str(e)}")
        
        # === NOW create CI summary from the actual data we just loaded ===
        report_data['ci_summary'] = {
            'pdca_cards': len(report_data['pdca_cards']),
            's6_findings': len(report_data['s6_findings']),
            'meeting_actions': len(report_data['meeting_actions']),
            'ci_records': len(report_data['ci_records'])
        }
        
        logger.info(f"=== CI Summary Calculated ===")
        logger.info(f"PDCA Cards: {report_data['ci_summary']['pdca_cards']}")
        logger.info(f"6S Findings: {report_data['ci_summary']['s6_findings']}")
        logger.info(f"Meeting Actions: {report_data['ci_summary']['meeting_actions']}")
        logger.info(f"CI Records: {report_data['ci_summary']['ci_records']}")
        
        # Fetch detailed data for other sections
        try:
            if safety_db:
                incidents_df = safety_db.get_recent_incidents()
                if not incidents_df.empty:
                    incidents_list = []
                    for _, row in incidents_df.iterrows():
                        incidents_list.append({
                            'time': str(row.get('time', 'N/A')),
                            'incidentType': str(row.get('incidentType', 'N/A')),
                            'location': str(row.get('location', 'N/A')),
                            'description': str(row.get('description', 'N/A')),
                            'inmediateAction': str(row.get('inmediateAction', 'N/A')),
                            'status': str(row.get('status', 'Open'))
                        })
                    report_data['safety_incidents'] = incidents_list[:10]
        except Exception as e:
            logger.error(f"Error fetching safety incidents: {str(e)}")
        
        try:
            if quality_db:
                alerts = quality_db.get_active_quality_alerts()
                if isinstance(alerts, list):
                    report_data['quality_alerts'] = alerts[:10]
                else:
                    alerts_list = []
                    for _, row in alerts.iterrows():
                        alerts_list.append({
                            'idQA': str(row.get('idQA', 'N/A')),
                            'defect': str(row.get('defect', 'N/A')),
                            'fechaFinal': str(row.get('fechaFinal', 'N/A')),
                            'qualityAuth': str(row.get('qualityAuth', 'N/A')),
                            'createdBy': str(row.get('createdBy', 'N/A'))
                        })
                    report_data['quality_alerts'] = alerts_list[:10]
        except Exception as e:
            logger.error(f"Error fetching quality alerts: {str(e)}")
        
        try:
            if production_db:
                downtime = production_db.get_downtime_events(1, 'all')
                if isinstance(downtime, list):
                    report_data['downtime_events'] = downtime[:10]
        except Exception as e:
            logger.error(f"Error fetching downtime events: {str(e)}")
        
        try:
            if logistics_db:
                material_dt = logistics_db.get_material_downtime(1)
                if isinstance(material_dt, list):
                    report_data['material_downtime'] = material_dt[:10]
        except Exception as e:
            logger.error(f"Error fetching material downtime: {str(e)}")
        
        logger.info(f"=== Report Generation Complete ===")
        logger.info(f"Total sections with data: Safety={bool(report_data['safety_summary'])}, "
                   f"Quality={bool(report_data['quality_summary'])}, "
                   f"Production={bool(report_data['production_summary'])}, "
                   f"Logistics={bool(report_data['logistics_summary'])}, "
                   f"CI={bool(report_data['ci_summary'])}")
        
        return render_template('comprehensive_report.html', data=report_data)
        
    except Exception as e:
        logger.error(f"Error generating comprehensive report: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"Error generating report: {str(e)}", 500

from flask import Flask, jsonify, request, send_file
import subprocess
import platform
import os
from datetime import datetime

@app.route('/open-historic-pdf')
def open_historic_pdf():
    """Serves the PDF file for viewing/download in the browser - FIXED to work from any computer"""
    selected_date = request.args.get('date', '')
    
    if not selected_date:
        return jsonify({'success': False, 'error': 'No date provided'})
    
    try:
        # Parse the date
        date_obj = datetime.strptime(selected_date, '%Y-%m-%d')
        year = date_obj.year
        month_name = date_obj.strftime('%B')
        formatted_date = date_obj.strftime('%Y-%m-%d')
        
        # Construct the PDF filename
        filename = f'Daily_OpEx_Report_{formatted_date}.pdf'
        
        # Use U: drive path
        folder_path = f'U:\\Production\\Daily OpEx Report\\{month_name} {year}'
        pdf_path = os.path.join(folder_path, filename)
        
        # Normalize path for Windows
        pdf_path = os.path.normpath(pdf_path)
        
        # Check if file exists
        if not os.path.exists(pdf_path):
            return jsonify({
                'success': False, 
                'error': 'PDF not found',
                'path': pdf_path,
                'filename': filename
            })
        
        # FIXED: Send the file to the browser instead of opening it on the server
        # This works from ANY computer accessing the Flask app
        return send_file(
            pdf_path,
            mimetype='application/pdf',
            as_attachment=False,  # Opens in browser instead of forcing download
            download_name=filename
        )
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/open-folder')
def open_folder():
    folder_path = request.args.get('path', '')
    
    if not folder_path:
        return jsonify({'success': False, 'error': 'No path provided'})
    
    try:
        if platform.system() == 'Windows':
            folder_path = os.path.normpath(folder_path)
            subprocess.Popen(['explorer', folder_path])
            return jsonify({'success': True, 'message': 'Folder opened'})
        else:
            return jsonify({'success': False, 'error': 'Only Windows is supported'})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})



@app.route('/save_pdf', methods=['POST'])
def save_pdf():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    report_type = request.form.get('report_type')
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    try:
        # Get current date for folder organization
        from datetime import datetime
        current_date = datetime.now()
        month_year = current_date.strftime('%B %Y')  # e.g., "October 2025"
        
        # Choose base path based on report type
        if report_type == 'daily_opex':
            base_path = r'U:\Production\Daily OpEX Report'
        else:
            base_path = r'U:\Production\Daily OpEX Report'
        
        # Create full path with month/year folder
        network_path = os.path.join(base_path, month_year)
        
        # Create directory if it doesn't exist
        if not os.path.exists(network_path):
            os.makedirs(network_path)

        filename = file.filename
        file_path = os.path.join(network_path, filename)
        file.save(file_path)
        
        return jsonify({
            'message': 'File saved successfully', 
            'filename': filename,
            'path': network_path
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

# Add these imports at the top of app.py if not already there
import glob
import traceback
import re

# Add this configuration near your other app.config settings
app.config['EXCEL_FOLDER'] = r'C:\Users\sanchel\BENTELER Group\Spartanburg Logistics - Backlog tracker'

# Excel Import Helper Functions
def find_latest_backlog_file():
    """Find the most recent 'Backlog tracker' Excel file from OneDrive/Teams"""
    try:
        excel_folder = app.config['EXCEL_FOLDER']
        
        # Check if folder exists
        if not os.path.exists(excel_folder):
            logger.error(f"OneDrive folder not found: {excel_folder}")
            logger.error("Make sure OneDrive is syncing and you're logged in")
            return None
        
        # Look for files matching pattern
        pattern = os.path.join(excel_folder, "Backlog tracker*.xlsx")
        files = glob.glob(pattern)
        
        # Also check for the exact filename we see in the screenshot
        exact_file = os.path.join(excel_folder, "Backlog tracker OCTOBER 2025.xlsx")
        if os.path.exists(exact_file) and exact_file not in files:
            files.append(exact_file)
        
        if not files:
            logger.error(f"No backlog tracker files found in {excel_folder}")
            # List what files ARE there for debugging
            try:
                available_files = os.listdir(excel_folder)
                logger.error(f"Available files: {available_files}")
            except:
                pass
            return None
        
        # Get the most recent file
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"✓ Found backlog file: {latest_file}")
        logger.info(f"  Last modified: {datetime.fromtimestamp(os.path.getmtime(latest_file))}")
        return latest_file
        
    except Exception as e:
        logger.error(f"Error finding backlog file: {e}")
        logger.error(traceback.format_exc())
        return None

def find_latest_backlog_sheet(xl_file):
    """Find the latest dated Backlog sheet (e.g., 'Backlog 10.15')"""
    backlog_sheets = []
    
    # Pattern to match "Backlog MM.DD" or "Backlog MM.D" or "Backlog M.DD"
    pattern = r'backlog\s+(\d{1,2})\.(\d{1,2})'
    
    for sheet in xl_file.sheet_names:
        match = re.search(pattern, sheet.lower())
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            backlog_sheets.append({
                'name': sheet,
                'month': month,
                'day': day,
                'sort_key': (month * 100) + day  # For sorting: 10.15 = 1015
            })
    
    if not backlog_sheets:
        logger.error(f"No dated Backlog sheets found. Available sheets: {xl_file.sheet_names}")
        return None
    
    # Sort by date and get the latest
    backlog_sheets.sort(key=lambda x: x['sort_key'], reverse=True)
    latest_sheet = backlog_sheets[0]
    
    logger.info(f"Latest sheet: {latest_sheet['name']} ({latest_sheet['month']}/{latest_sheet['day']})")
    return latest_sheet['name']

def read_backlog_data(file_path):
    """Read supplier data from the latest Backlog sheet - INCLUDING PROJECT COLUMN"""
    xl_file = None
    try:
        xl_file = pd.ExcelFile(file_path)
        
        # Find the latest dated backlog sheet
        backlog_sheet = find_latest_backlog_sheet(xl_file)
        if not backlog_sheet:
            return None
        
        logger.info(f"Reading from sheet: '{backlog_sheet}'")
        
        # Read the data - NO HEADER, we'll use column positions
        # Column A (0) = Project, Column E (4) = Supplier, Column F (5) = Risk, Column H (7) = Recovery plan
        df = pd.read_excel(
            file_path, 
            sheet_name=backlog_sheet,
            header=None,
            skiprows=2,
            usecols=[1, 4, 5, 7],  # Added column 0 for Project
            dtype=str
        )
        
        # Assign column names manually
        df.columns = ['Project', 'Supplier', 'Risk', 'Recovery plan']
        
        # CLEAN PROJECT COLUMN
        df['Project'] = df['Project'].fillna('N/A')
        df['Project'] = df['Project'].astype(str).str.strip()
        # Replace invalid project values
        invalid_patterns = ['#N/A', '#NA', 'NAN', 'NONE', '', 'UNNAMED']
        mask = df['Project'].str.upper().isin(invalid_patterns) | df['Project'].str.startswith('#')
        df.loc[mask, 'Project'] = 'N/A'
        
        # CLEAN SUPPLIER COLUMN
        df = df[df['Supplier'].notna()]
        df['Supplier'] = df['Supplier'].astype(str).str.strip()
        
        # Remove invalid supplier values
        invalid_patterns = ['#N/A', '#NA', 'N/A', 'NA', 'NAN', 'NONE', 'SUPPLIER', '', 'UNNAMED']
        df = df[~df['Supplier'].str.upper().isin(invalid_patterns)]
        df = df[~df['Supplier'].str.startswith('#')]
        df = df[~df['Supplier'].str.upper().str.startswith('UNNAMED')]
        df = df[df['Supplier'].str.strip() != '']
        
        if len(df) == 0:
            logger.error("No valid supplier data found after cleaning")
            return None
        
        # CLEAN RISK COLUMN
        def standardize_risk(risk_val):
            if pd.isna(risk_val):
                return 'Medium'
            risk_str = str(risk_val).strip().upper()
            if risk_str.startswith('#') or risk_str in ['NAN', '', 'NONE', 'N/A']:
                return 'Medium'
            if risk_str in ['HIGH', 'HI', 'H', 'RED']:
                return 'High'
            elif risk_str in ['LOW', 'LO', 'L', 'GREEN', 'GRN']:
                return 'Low'
            elif risk_str in ['MED', 'MEDIUM', 'MID', 'M', 'YELLOW', 'YLW']:
                return 'Medium'
            return 'Medium'
        
        df['Risk'] = df['Risk'].apply(standardize_risk)
        
        # CLEAN RECOVERY PLAN COLUMN
        df['Recovery plan'] = df['Recovery plan'].fillna('No recovery plan specified')
        df['Recovery plan'] = df['Recovery plan'].astype(str).str.strip()
        empty_patterns = ['nan', '', 'None', 'N/A', '#N/A', '#NA', 'NaN', 'NONE']
        mask = (df['Recovery plan'].str.upper().isin([p.upper() for p in empty_patterns])) | \
               (df['Recovery plan'].str.startswith('#'))
        df.loc[mask, 'Recovery plan'] = 'No recovery plan specified'
        
        logger.info(f"✓ Processed {len(df)} entries from {df['Supplier'].nunique()} unique suppliers")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading Excel: {e}")
        logger.error(traceback.format_exc())
        return None
    finally:
        # CRITICAL: Always close the Excel file
        if xl_file is not None:
            try:
                xl_file.close()
                logger.info("Excel file closed successfully")
            except Exception as close_error:
                logger.error(f"Error closing Excel file: {close_error}")

def deduplicate_suppliers(df):
    """Group by supplier and consolidate data - INCLUDING PROJECT"""
    risk_priority = {'High': 3, 'Medium': 2, 'Low': 1}
    
    grouped = df.groupby('Supplier').agg({
        'Project': lambda x: ', '.join(sorted(set([str(p).strip() for p in x if pd.notna(p) and str(p).strip() not in ['', 'N/A']]))),
        'Risk': lambda x: max(x, key=lambda r: risk_priority.get(r, 0)),
        'Recovery plan': lambda x: ' | '.join([str(p) for p in x if pd.notna(p) and str(p).strip() != ''])
    }).reset_index()
    
    # If no valid projects, set to N/A
    grouped.loc[grouped['Project'] == '', 'Project'] = 'N/A'
    
    logger.info(f"Reduced to {len(grouped)} unique suppliers")
    return grouped


def get_supplier_number_from_logistics_planner(supplier_name, conn):
    """Get supplier number from Logistics_Planner table with improved matching"""
    try:
        cursor = conn.cursor()
        clean_name = supplier_name.strip().upper()
        
        # 1. Try exact match first
        query = """
            SELECT TOP 1 VendorID, Vendor
            FROM dbo.Logistics_Planner 
            WHERE UPPER(LTRIM(RTRIM(Vendor))) = ?
        """
        cursor.execute(query, (clean_name,))
        row = cursor.fetchone()
        if row:
            logger.info(f"Exact match: {supplier_name} -> {row[0]}")
            return row[0]
        
        # 2. Try without punctuation (removes commas, periods, etc)
        clean_name_no_punct = re.sub(r'[,.\-\s]+', ' ', clean_name).strip()
        query = """
            SELECT TOP 1 VendorID, Vendor
            FROM dbo.Logistics_Planner 
            WHERE UPPER(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Vendor)), ',', ' '), '.', ' '), '-', ' ')) 
            LIKE ?
        """
        cursor.execute(query, (f'%{clean_name_no_punct}%',))
        row = cursor.fetchone()
        if row:
            logger.info(f"Punctuation-insensitive match: {supplier_name} -> {row[1]} ({row[0]})")
            return row[0]
        
        # 3. Try partial match (contains)
        query = """
            SELECT TOP 1 VendorID, Vendor
            FROM dbo.Logistics_Planner 
            WHERE UPPER(LTRIM(RTRIM(Vendor))) LIKE ?
        """
        cursor.execute(query, (f'%{clean_name}%',))
        row = cursor.fetchone()
        if row:
            logger.info(f"Partial match: {supplier_name} -> {row[1]} ({row[0]})")
            return row[0]
        
        # 4. Try first significant word match (for cases like "BMW AG" vs "BMW GMBH")
        first_word = clean_name.split()[0] if clean_name.split() else clean_name
        if len(first_word) >= 3:  # Only if word is meaningful
            query = """
                SELECT TOP 1 VendorID, Vendor
                FROM dbo.Logistics_Planner 
                WHERE UPPER(LTRIM(RTRIM(Vendor))) LIKE ?
            """
            cursor.execute(query, (f'{first_word}%',))
            row = cursor.fetchone()
            if row:
                logger.warning(f"First-word match (verify!): {supplier_name} -> {row[1]} ({row[0]})")
                return row[0]
        
        logger.error(f"❌ NO MATCH FOUND: {supplier_name}")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching supplier number: {e}")
        return None

def check_existing_supplier(supplier_number, conn):
    """Check if supplier already exists in DailySuppliers table"""
    try:
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM dbo.DailySuppliers WHERE Number = ?"
        cursor.execute(query, (supplier_number,))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.error(f"Error checking existing supplier: {e}")
        return False

def import_supplier_to_db(supplier_name, supplier_number, project, risk, actions, conn):
    """Import or update a supplier in the database (UPSERT logic) - INCLUDING PROJECT"""
    try:
        cursor = conn.cursor()
        
        # Check if supplier already exists
        if check_existing_supplier(supplier_number, conn):
            # Get existing project value
            existing_project_query = """
                SELECT Project FROM dbo.DailySuppliers 
                WHERE Number = ?
            """
            cursor.execute(existing_project_query, (supplier_number,))
            existing_row = cursor.fetchone()
            existing_project = existing_row[0] if existing_row and existing_row[0] else None
            
            # Merge projects intelligently
            if existing_project and project:
                # Split existing projects, add new project, remove duplicates
                existing_projects = set([p.strip() for p in str(existing_project).split(',') if p.strip() not in ['', 'N/A']])
                new_projects = set([p.strip() for p in str(project).split(',') if p.strip() not in ['', 'N/A']])
                all_projects = existing_projects.union(new_projects)
                merged_project = ', '.join(sorted(all_projects))
            elif project:
                merged_project = project
            else:
                merged_project = existing_project
            
            # Truncate if too long (adjust max_length to your DB schema)
            max_project_length = 100  # Check your actual DB column size
            max_actions_length = 500  # Check your actual DB column size
            
            if merged_project and len(str(merged_project)) > max_project_length:
                logger.warning(f"Project list too long for {supplier_name}, truncating from: {merged_project}")
                merged_project = str(merged_project)[:max_project_length-3] + '...'
            
            # UPDATE existing supplier with merged data
            update_query = """
                UPDATE dbo.DailySuppliers 
                SET Project = ?,
                    Risk = ?, 
                    Actions = ?,
                    CreatedDate = GETDATE(),
                    CreatedBy = ?
                WHERE Number = ?
            """
            
            clean_actions = actions if actions and str(actions).strip() != '' else 'No actions specified'
            
            # Truncate actions if too long
            if len(str(clean_actions)) > max_actions_length:
                clean_actions = str(clean_actions)[:max_actions_length-3] + '...'
            
            cursor.execute(update_query, (
                merged_project,
                risk,
                clean_actions,
                'Excel Import - Updated',
                supplier_number
            ))
            
            conn.commit()
            logger.info(f"✓ Updated existing supplier: {supplier_name} ({supplier_number}) - Projects: {merged_project}")
            return {'status': 'updated', 'reason': 'existing_supplier_updated'}
        else:
            # INSERT new supplier
            insert_query = """
                INSERT INTO dbo.DailySuppliers 
                (Number, Project, Risk, Actions, CreatedDate, CreatedBy)
                VALUES (?, ?, ?, ?, GETDATE(), ?)
            """
            
            clean_actions = actions if actions and str(actions).strip() != '' else 'No actions specified'
            clean_project = project if project and str(project).strip() not in ['', 'N/A'] else None
            
            cursor.execute(insert_query, (
                supplier_number,
                clean_project,
                risk,
                clean_actions,
                'Excel Import'
            ))
            
            conn.commit()
            logger.info(f"✓ Successfully imported new supplier: {supplier_name} - Project: {clean_project}")
            return {'status': 'success', 'reason': 'new_supplier_added'}
        
    except Exception as e:
        logger.error(f"Error importing/updating supplier {supplier_name}: {e}")
        logger.error(traceback.format_exc())
        return {'status': 'error', 'reason': str(e)}

@app.route('/api/logistics/import-from-excel', methods=['POST'])
def import_from_excel():
    """Import suppliers from Excel and remove suppliers not in Excel (FULL SYNC) - INCLUDING PROJECT"""
    try:
        # Find the latest Excel file
        file_path = find_latest_backlog_file()
        if not file_path:
            return jsonify({
                'error': f'No backlog tracker file found in {app.config["EXCEL_FOLDER"]}',
                'success': False
            }), 404
        
        # Read data from Excel
        df = read_backlog_data(file_path)
        if df is None or len(df) == 0:
            return jsonify({
                'error': 'Could not read data from Excel file or no valid data found',
                'success': False
            }), 400
        
        # Deduplicate suppliers
        df = deduplicate_suppliers(df)
        
        # Connect to TEST database
        conn = get_db_connection_1()
        cursor = conn.cursor()
        
        # Get all supplier numbers from Excel
        excel_supplier_numbers = []
        for idx, row in df.iterrows():
            supplier_name = row['Supplier']
            supplier_number = get_supplier_number_from_logistics_planner(supplier_name, conn)
            if supplier_number:
                excel_supplier_numbers.append(supplier_number)
        
        # STEP 1: Delete suppliers that are NOT in Excel anymore
        deleted_count = 0
        if excel_supplier_numbers:
            # Build a NOT IN clause
            placeholders = ','.join(['?' for _ in excel_supplier_numbers])
            delete_query = f"""
                DELETE FROM dbo.DailySuppliers 
                WHERE Number NOT IN ({placeholders})
            """
            cursor.execute(delete_query, excel_supplier_numbers)
            deleted_count = cursor.rowcount
            conn.commit()
            logger.info(f"Deleted {deleted_count} suppliers not in Excel")
        
        # STEP 2: Import/Update suppliers from Excel
        results = {
            'total': len(df),
            'success': 0,      # New suppliers added
            'updated': 0,      # Existing suppliers updated
            'deleted': deleted_count,  # Suppliers removed (not in Excel)
            'failed': 0,
            'details': []
        }
        
        for idx, row in df.iterrows():
            supplier_name = row['Supplier']
            project = row['Project']
            risk = row['Risk']
            actions = row['Recovery plan']
            
            # Get supplier number from Logistics_Planner
            supplier_number = get_supplier_number_from_logistics_planner(supplier_name, conn)
            
            if not supplier_number:
                results['failed'] += 1
                results['details'].append({
                    'supplier': supplier_name,
                    'project': project,
                    'number': 'N/A',
                    'status': 'failed',
                    'reason': 'Supplier not found in Logistics_Planner'
                })
                continue
            
            # Import to database (will update if exists, insert if new)
            result = import_supplier_to_db(supplier_name, supplier_number, project, risk, actions, conn)
            
            if result['status'] == 'success':
                results['success'] += 1
            elif result['status'] == 'updated':
                results['updated'] += 1
            else:
                results['failed'] += 1
            
            results['details'].append({
                'supplier': supplier_name,
                'project': project,
                'number': supplier_number,
                'status': result['status'],
                'reason': result.get('reason', '')
            })
        
        conn.close()
        
        # Prepare response message
        message = f"Sync complete: {results['success']} new, {results['updated']} updated, {results['deleted']} removed, {results['failed']} failed"
        
        return jsonify({
            'success': True,
            'message': message,
            'results': results,
            'file': os.path.basename(file_path)
        })
        
    except Exception as e:
        logger.error(f"Error in Excel import: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            'error': str(e),
            'success': False
        }), 500
    


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)