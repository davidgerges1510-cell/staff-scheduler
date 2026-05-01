from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, date, timedelta
from functools import wraps
import calendar, os, threading, smtplib, json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'staff-sched-secret-xk29')

# ── Database: use PostgreSQL on Render, SQLite locally ─────────────────────────
basedir = os.path.abspath(os.path.dirname(__file__))
_db_url = os.environ.get('DATABASE_URL', '')
if _db_url.startswith('postgres://'):          # Render gives old-style prefix
    _db_url = _db_url.replace('postgres://', 'postgresql://', 1)
if not _db_url:
    os.makedirs(os.path.join(basedir, 'instance'), exist_ok=True)
    _db_url = 'sqlite:///' + os.path.join(basedir, 'instance', 'scheduler.db')

app.config['SQLALCHEMY_DATABASE_URI'] = _db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True,
    'pool_recycle':  300,
}
db = SQLAlchemy(app)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
SHIFTS = {
    'D':   {'name': 'Day',              'icon': '☀️',  'bg': '#c6f6d5', 'color': '#276749'},
    'N':   {'name': 'Night',            'icon': '🌙',  'bg': '#e9d8fd', 'color': '#553c9a'},
    'DSS': {'name': 'Day Senior',       'icon': '☀️⭐','bg': '#f0fff4', 'color': '#22543d'},
    'NSS': {'name': 'Night Senior',     'icon': '🌙⭐','bg': '#faf5ff', 'color': '#322659'},
    'DS':  {'name': 'Day Trainee',      'icon': '🌤️', 'bg': '#fefcbf', 'color': '#744210'},
    'NS':  {'name': 'Night Trainee',    'icon': '🌛',  'bg': '#e9d8fd', 'color': '#44337a'},
    'BL':  {'name': 'Sick Leave',       'icon': '🤒',  'bg': '#fed7d7', 'color': '#9b2c2c'},
    'OO':  {'name': 'Vacation',         'icon': '✈️',  'bg': '#bee3f8', 'color': '#2a4365'},
    'OS':  {'name': 'Vacation Unpaid',  'icon': '🏠',  'bg': '#fffbeb', 'color': '#744210'},
    'DOF': {'name': 'Day Off',          'icon': '😴',  'bg': '#f7fafc', 'color': '#718096'},
}
LEAVE_TYPES = {'annual': 'Annual Leave', 'sick': 'Sick Leave', 'personal': 'Personal Leave', 'unpaid': 'Unpaid Leave'}
SHIFT_PATTERNS = [
    ['D','N','DOF','D','N','DOF','D'],
    ['N','DOF','D','N','DOF','D','N'],
    ['DOF','D','N','DOF','D','N','DOF'],
    ['D','D','N','DOF','D','N','DOF'],
]
COLORS = ['#e53e3e','#dd6b20','#d69e2e','#38a169','#3182ce','#805ad5',
          '#d53f8c','#319795','#2d3748','#c05621','#276749','#6b46c1']
MONTHS = ['January','February','March','April','May','June',
          'July','August','September','October','November','December']

# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────
class User(db.Model):
    id           = db.Column(db.Integer, primary_key=True)
    name         = db.Column(db.String(100), nullable=False)
    username     = db.Column(db.String(50), unique=True, nullable=False)
    email        = db.Column(db.String(120), default='')
    password_hash= db.Column(db.String(256))
    role         = db.Column(db.String(20), default='employee')
    department   = db.Column(db.String(100), default='')
    location     = db.Column(db.String(100), default='')
    office       = db.Column(db.String(100), default='')
    phone        = db.Column(db.String(50), default='')
    color        = db.Column(db.String(20), default='#3182ce')
    vac_days     = db.Column(db.Integer, default=21)
    sick_days    = db.Column(db.Integer, default=14)
    shift_pattern= db.Column(db.Integer, default=0)
    day_hours    = db.Column(db.Float, default=12.0)
    night_hours  = db.Column(db.Float, default=12.0)
    sort_order   = db.Column(db.Integer, default=0)
    ntfy_topic      = db.Column(db.String(100), default='')
    telegram_chat_id = db.Column(db.String(50), default='')
    is_active    = db.Column(db.Boolean, default=True)
    created_at   = db.Column(db.DateTime, default=datetime.utcnow)

    requests      = db.relationship('Request', foreign_keys='Request.user_id',
                                    backref='user', cascade='all, delete-orphan')
    notifications = db.relationship('Notification', backref='user', cascade='all, delete-orphan')
    schedules     = db.relationship('Schedule', backref='user', cascade='all, delete-orphan')

    def set_password(self, pw):
        self.password_hash = generate_password_hash(pw or '')

    def check_password(self, pw):
        if not self.password_hash:
            return (pw or '') == ''
        return check_password_hash(self.password_hash, pw or '')

    def initials(self):
        return ''.join(p[0].upper() for p in self.name.strip().split()[:2])

    def to_dict(self):
        return dict(id=self.id, name=self.name, username=self.username,
                    email=self.email or '', role=self.role,
                    department=self.department or '', location=self.location or '',
                    office=self.office or '', phone=self.phone or '',
                    color=self.color, vac_days=self.vac_days, sick_days=self.sick_days,
                    day_hours=self.day_hours or 12.0, night_hours=self.night_hours or 12.0,
                    sort_order=self.sort_order or 0,
                    ntfy_topic=self.ntfy_topic or '',
                    initials=self.initials())


class Schedule(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    year       = db.Column(db.Integer, nullable=False)
    month      = db.Column(db.Integer, nullable=False)
    day        = db.Column(db.Integer, nullable=False)
    shift_code = db.Column(db.String(10), default='DOF')
    hours      = db.Column(db.Float, nullable=True)   # custom hours override
    __table_args__ = (db.UniqueConstraint('user_id', 'year', 'month', 'day'),)


class Request(db.Model):
    id               = db.Column(db.Integer, primary_key=True)
    type             = db.Column(db.String(20), nullable=False)   # leave | swap | draft
    user_id          = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    # leave
    leave_type       = db.Column(db.String(20))
    start_date       = db.Column(db.Date)
    end_date         = db.Column(db.Date)
    days_count       = db.Column(db.Integer)
    # swap
    target_user_id   = db.Column(db.Integer, db.ForeignKey('user.id'))
    swap_date        = db.Column(db.Date)
    user_shift       = db.Column(db.String(10))
    target_shift     = db.Column(db.String(10))
    # draft
    draft_date       = db.Column(db.Date)
    proposed_shift   = db.Column(db.String(10))
    current_shift_code = db.Column(db.String(10))
    # common
    reason           = db.Column(db.Text, default='')
    status           = db.Column(db.String(20), default='pending')
    admin_note       = db.Column(db.Text, default='')
    created_at       = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at       = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    target_user      = db.relationship('User', foreign_keys=[target_user_id])

    def to_dict(self):
        u = self.user
        tu = self.target_user
        return dict(
            id=self.id, type=self.type, status=self.status,
            user_id=self.user_id, user_name=u.name if u else '',
            user_initials=u.initials() if u else '', user_color=u.color if u else '#888',
            leave_type=self.leave_type or '', days_count=self.days_count or 0,
            start_date=str(self.start_date) if self.start_date else '',
            end_date=str(self.end_date) if self.end_date else '',
            target_user_id=self.target_user_id, target_user_name=tu.name if tu else '',
            swap_date=str(self.swap_date) if self.swap_date else '',
            user_shift=self.user_shift or '', target_shift=self.target_shift or '',
            draft_date=str(self.draft_date) if self.draft_date else '',
            proposed_shift=self.proposed_shift or '',
            current_shift_code=self.current_shift_code or '',
            reason=self.reason or '', admin_note=self.admin_note or '',
            created_at=self.created_at.strftime('%Y-%m-%d %H:%M') if self.created_at else '',
        )


class Notification(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    type       = db.Column(db.String(20), default='info')
    message    = db.Column(db.Text, nullable=False)
    is_read    = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return dict(id=self.id, type=self.type, message=self.message,
                    is_read=self.is_read,
                    created_at=self.created_at.strftime('%Y-%m-%d %H:%M'))


class AppSettings(db.Model):
    id    = db.Column(db.Integer, primary_key=True)
    key   = db.Column(db.String(50), unique=True, nullable=False)
    value = db.Column(db.Text, default='')

    @staticmethod
    def get(key, default=''):
        s = AppSettings.query.filter_by(key=key).first()
        return s.value if s else default

    @staticmethod
    def put(key, value):
        s = AppSettings.query.filter_by(key=key).first()
        if s:
            s.value = str(value)
        else:
            db.session.add(AppSettings(key=key, value=str(value)))
        db.session.commit()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def wrapper(*a, **kw):
        if 'user_id' not in session:
            if request.is_json:
                return jsonify(error='Not authenticated'), 401
            return redirect(url_for('login'))
        u = current_user()
        if not u:
            session.clear()
            if request.is_json:
                return jsonify(error='Not authenticated'), 401
            return redirect(url_for('login'))
        return f(*a, **kw)
    return wrapper

def admin_required(f):
    @wraps(f)
    def wrapper(*a, **kw):
        if 'user_id' not in session:
            return jsonify(error='Not authenticated'), 401
        u = db.session.get(User, session['user_id'])
        if not u or u.role != 'admin':
            return jsonify(error='Admin only'), 403
        return f(*a, **kw)
    return wrapper

def current_user():
    uid = session.get('user_id')
    return db.session.get(User, uid) if uid else None

def ensure_schedule(user, year, month):
    """No longer auto-fills schedule — empty days stay blank."""
    pass  # schedule cells are blank until explicitly set or imported
    db.session.commit()

def add_notification(user_id, ntype, message):
    db.session.add(Notification(user_id=user_id, type=ntype, message=message))
    db.session.commit()

def send_telegram_notification(title, message, chat_id=None):
    """Send Telegram notification. Uses admin chat_id by default."""
    def _send():
        try:
            import urllib.request as _ur, urllib.parse as _up
            token   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
            cid     = chat_id or os.environ.get('TELEGRAM_CHAT_ID', '')
            if not token or not cid:
                return
            text = f"*{title}*\n{message}"
            url  = f"https://api.telegram.org/bot{token}/sendMessage"
            data = _up.urlencode({'chat_id': cid, 'text': text, 'parse_mode': 'Markdown'}).encode()
            _ur.urlopen(_ur.Request(url, data=data), timeout=10)
            print(f'[TELEGRAM OK] Sent to {cid}: {title}')
        except Exception as e:
            print(f'[TELEGRAM ERROR] {e}')
    threading.Thread(target=_send, daemon=True).start()

def send_push_notification(title, message, priority='default'):
    """Send push notification via ntfy.sh"""
    def _push():
        with app.app_context():
            try:
                topic = AppSettings.get('ntfy_topic', '') or os.environ.get('NTFY_TOPIC', '')
                if not topic: return
                import urllib.request as _ur
                url = f'https://ntfy.sh/{topic}'
                req = _ur.Request(url, data=message.encode('utf-8'),
                                  method='POST',
                                  headers={
                                      'Title': title,
                                      'Priority': '3',
                                      'Tags': 'bell'
                                  })
                _ur.urlopen(req, timeout=8)
            except Exception as e:
                print(f'[PUSH ERROR] {e}')
    threading.Thread(target=_push, daemon=True).start()

def send_email_sync(to_email, subject, html_body):
    """Send email synchronously (used for notifications)."""
    try:
        smtp_srv  = AppSettings.get('smtp_server', 'smtp.gmail.com')
        smtp_port = int(AppSettings.get('smtp_port', '587'))
        smtp_user = AppSettings.get('smtp_user', '')
        smtp_pass = AppSettings.get('smtp_pass', '')
        from_name = AppSettings.get('from_name', 'Staff Scheduler')
        enabled   = AppSettings.get('email_enabled', 'false') == 'true'
        if not enabled or not smtp_user or not to_email:
            return
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From']    = f'{from_name} <{smtp_user}>'
        msg['To']      = to_email
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        with smtplib.SMTP(smtp_srv, smtp_port) as srv:
            srv.ehlo(); srv.starttls(); srv.ehlo()
            srv.login(smtp_user, smtp_pass)
            srv.sendmail(smtp_user, to_email, msg.as_string())
        print(f'[EMAIL OK] Sent to {to_email}')
    except Exception as e:
        print(f'[EMAIL ERROR] {e}')

def send_email_async(to_email, subject, html_body):
    def _send():
        with app.app_context():
            try:
                smtp_srv  = AppSettings.get('smtp_server', 'smtp.gmail.com')
                smtp_port = int(AppSettings.get('smtp_port', '587'))
                smtp_user = AppSettings.get('smtp_user', '')
                smtp_pass = AppSettings.get('smtp_pass', '')
                from_name = AppSettings.get('from_name', 'Staff Scheduler')
                enabled   = AppSettings.get('email_enabled', 'false') == 'true'
                if not enabled or not smtp_user or not to_email:
                    return
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From']    = f'{from_name} <{smtp_user}>'
                msg['To']      = to_email
                msg.attach(MIMEText(html_body, 'html', 'utf-8'))
                with smtplib.SMTP(smtp_srv, smtp_port) as srv:
                    srv.ehlo(); srv.starttls(); srv.ehlo()
                    srv.login(smtp_user, smtp_pass)
                    srv.sendmail(smtp_user, to_email, msg.as_string())
                print(f'[EMAIL OK] Sent to {to_email}')
            except Exception as e:
                print(f'[EMAIL ERROR] {e}')
    threading.Thread(target=_send, daemon=True).start()

def notify_admin_new_request(emp, req):
    """Send email + push notification to admin when employee submits a new request."""
    admin = User.query.filter_by(role='admin').first()
    type_map = {'leave': '✈️ Leave Request', 'swap': '🔄 Swap Request', 'draft': '✏️ Schedule Proposal'}
    rtype = type_map.get(req.type, req.type.capitalize())
    if req.type == 'leave':
        lt     = LEAVE_TYPES.get(req.leave_type, req.leave_type or 'Leave')
        detail = f'{lt} — {req.start_date} → {req.end_date} ({req.days_count} days)'
    elif req.type == 'swap':
        tu     = db.session.get(User, req.target_user_id)
        detail = f'Swap with {tu.name if tu else "?"} on {req.swap_date}'
    else:
        sh     = SHIFTS.get(req.proposed_shift, {}).get('name', req.proposed_shift)
        detail = f'Proposal: {sh} on {req.draft_date}'

    # Telegram notification
    send_telegram_notification(
        title   = f'⏳ طلب جديد — {emp.name}',
        message = detail
    )

    # Push notification (ntfy.sh)
    send_push_notification(
        title   = f'⏳ New request — {emp.name}',
        message = detail,
        priority= 'high'
    )

    # Email to admin — use admin profile email or fallback to smtp_user
    admin_email = (admin.email if admin and admin.email else '') or AppSettings.get('smtp_user', '')
    if not admin_email: return
    subj = f'⏳ New {rtype} — {emp.name}'
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
      <div style="background:linear-gradient(135deg,#1a9e9e,#4A6FA5);padding:24px;border-radius:12px 12px 0 0">
        <h2 style="color:#fff;margin:0">⏳ New Request Submitted</h2>
      </div>
      <div style="padding:24px;background:#f9fafb;border:1px solid #e2e8f0">
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <tr><td style="padding:6px 0;color:#718096;width:40%">Employee</td><td style="font-weight:600">{emp.name}</td></tr>
          <tr><td style="padding:6px 0;color:#718096">Type</td><td>{rtype}</td></tr>
          <tr><td style="padding:6px 0;color:#718096">Details</td><td>{detail}</td></tr>
          <tr><td style="padding:6px 0;color:#718096">Reason</td><td>{req.reason or '—'}</td></tr>
          <tr><td style="padding:6px 0;color:#718096">Submitted</td><td>{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC</td></tr>
        </table>
        <p style="margin-top:16px;font-size:13px;color:#4a5568">Please log in to the system to review and approve or reject this request.</p>
      </div>
    </div>"""
    send_email_sync(admin_email, subj, html)

def notify_and_email(emp, req, admin_note=''):
    ok   = req.status == 'approved'
    icon = '✅' if ok else '❌'
    word = 'APPROVED' if ok else 'REJECTED'

    if req.type == 'leave':
        lt  = LEAVE_TYPES.get(req.leave_type, req.leave_type or 'Leave')
        msg = f"{icon} Your {lt} request ({req.days_count} days, {req.start_date} → {req.end_date}) was {word}."
        subj = f"Leave Request {word} — {lt}"
    elif req.type == 'swap':
        msg  = f"{icon} Your shift swap request on {req.swap_date} was {word}."
        subj = f"Swap Request {word}"
    else:
        sh  = SHIFTS.get(req.proposed_shift, {}).get('name', req.proposed_shift)
        msg = f"{icon} Your schedule proposal ({sh}) on {req.draft_date} was {word}."
        subj = f"Schedule Proposal {word}"

    if admin_note:
        msg += f' Note: {admin_note}'

    add_notification(emp.id, 'approved' if ok else 'rejected', msg)

    # Push notification to employee if they have ntfy topic
    if getattr(emp, 'ntfy_topic', ''):
        try:
            import urllib.request as _ur
            url = f'https://ntfy.sh/{emp.ntfy_topic}'
            _req = _ur.Request(url, data=msg.encode('utf-8'),
                               method='POST',
                               headers={'Title': f'Request {word}', 'Priority': '3', 'Tags': 'bell'})
            _ur.urlopen(_req, timeout=8)
        except Exception as e:
            print(f'[PUSH EMP ERROR] {e}')

    # Build HTML email
    bal_vac  = max(0, emp.vac_days  - Schedule.query.filter_by(user_id=emp.id, shift_code='OO').count())
    bal_sick = max(0, emp.sick_days - Schedule.query.filter_by(user_id=emp.id, shift_code='BL').count())
    color    = '#38a169' if ok else '#e53e3e'

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
      <div style="background:linear-gradient(135deg,#1a9e9e,#4A6FA5);padding:24px;border-radius:12px 12px 0 0">
        <h2 style="color:#fff;margin:0">Staff Scheduler — Request {word}</h2>
      </div>
      <div style="padding:24px;background:#f9fafb;border:1px solid #e2e8f0">
        <p style="font-size:16px;color:{color};font-weight:700">{icon} Request {word}</p>
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <tr><td style="padding:6px 0;color:#718096">Employee</td><td style="font-weight:600">{emp.name}</td></tr>
          <tr><td style="padding:6px 0;color:#718096">Type</td><td>{req.type.capitalize()}</td></tr>
          {'<tr><td style="padding:6px 0;color:#718096">Leave Type</td><td>'+LEAVE_TYPES.get(req.leave_type,req.leave_type or '')+'</td></tr>' if req.type=='leave' else ''}
          {'<tr><td style="padding:6px 0;color:#718096">Dates</td><td>'+str(req.start_date)+' → '+str(req.end_date)+' ('+str(req.days_count)+' days)</td></tr>' if req.type=='leave' else ''}
          {'<tr><td style="padding:6px 0;color:#718096">Date</td><td>'+str(req.swap_date or req.draft_date)+'</td></tr>' if req.type in ('swap','draft') else ''}
          <tr><td style="padding:6px 0;color:#718096">Reason</td><td>{req.reason or '—'}</td></tr>
          <tr><td style="padding:6px 0;color:#718096">Decision</td><td style="color:{color};font-weight:700">{word} on {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}</td></tr>
          {'<tr><td style="padding:6px 0;color:#718096">Admin Note</td><td style="font-style:italic">'+admin_note+'</td></tr>' if admin_note else ''}
        </table>
        <hr style="margin:16px 0;border:none;border-top:1px solid #e2e8f0">
        <p style="font-size:12px;color:#718096">Remaining Balance — Annual: <b>{bal_vac} days</b> | Sick: <b>{bal_sick} days</b></p>
      </div>
    </div>"""
    send_email_sync(emp.email, subj, html)

    # Telegram notification to employee
    if getattr(emp, 'telegram_chat_id', ''):
        send_telegram_notification(
            title   = f'{icon} طلبك {word}',
            message = msg,
            chat_id = emp.telegram_chat_id
        )

# ─────────────────────────────────────────────
# AUTH ROUTES
# ─────────────────────────────────────────────
@app.route('/telegram-webhook', methods=['POST'])
def telegram_webhook():
    """Handle Telegram bot messages — register employee chat_id on /start"""
    try:
        data    = request.get_json(silent=True) or {}
        message = data.get('message', {})
        text    = message.get('text', '')
        chat_id = str(message.get('chat', {}).get('id', ''))
        username = message.get('chat', {}).get('username', '')
        token   = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        import urllib.request as _ur, urllib.parse as _up

        if text.startswith('/start'):
            reply = f"مرحباً! 👋\nرقم Telegram الخاص بك هو:\n`{chat_id}`\n\nأرسله للمشرف ليربطه بحسابك في التطبيق."
            url  = f"https://api.telegram.org/bot{token}/sendMessage"
            _data = _up.urlencode({'chat_id': chat_id, 'text': reply, 'parse_mode': 'Markdown'}).encode()
            _ur.urlopen(_ur.Request(url, data=_data), timeout=10)
    except Exception as e:
        print(f'[WEBHOOK ERROR] {e}')
    return jsonify(ok=True)

@app.route('/manifest.json')
def pwa_manifest():
    from flask import Response
    manifest = {
        "name": "Staff Scheduler",
        "short_name": "Scheduler",
        "description": "Staff shift scheduling system",
        "start_url": "/dashboard",
        "display": "standalone",
        "background_color": "#f0f4f8",
        "theme_color": "#1a9e9e",
        "orientation": "any",
        "icons": [
            {"src": "https://img.icons8.com/fluency/192/calendar.png", "sizes": "192x192", "type": "image/png"},
            {"src": "https://img.icons8.com/fluency/512/calendar.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    import json as _json
    return Response(_json.dumps(manifest), mimetype='application/manifest+json')

@app.route('/sw.js')
def service_worker():
    from flask import Response
    sw = """
self.addEventListener('install', e => self.skipWaiting());
self.addEventListener('activate', e => clients.claim());
self.addEventListener('fetch', e => {
  if(e.request.method !== 'GET') return;
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
"""
    return Response(sw, mimetype='application/javascript')

@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip().lower()
        password = request.form.get('password') or ''
        if not username:
            return render_template('login.html', error='Please enter your username.')
        user = User.query.filter(db.func.lower(User.username) == username).first()
        if not user or not user.check_password(password):
            return render_template('login.html', error='Incorrect username or password.')
        session['user_id'] = user.id
        return redirect(url_for('dashboard'))
    if 'user_id' in session:
        u = current_user()
        if u:
            return redirect(url_for('dashboard'))
        session.clear()
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    u = current_user()
    if not u:
        return redirect(url_for('login'))
    return render_template('dashboard.html',
        user=u,
        shifts=SHIFTS,
        months=MONTHS,
        leave_types=LEAVE_TYPES,
        colors=COLORS,
        now=datetime.utcnow())

# ─────────────────────────────────────────────
# API — CURRENT USER
# ─────────────────────────────────────────────
@app.route('/api/me')
@login_required
def api_me():
    u = current_user()
    d = u.to_dict()
    d['unread_count'] = Notification.query.filter_by(user_id=u.id, is_read=False).count()
    return jsonify(d)

@app.route('/api/me/password', methods=['POST'])
@login_required
def api_change_password():
    u  = current_user()
    data = request.get_json()
    old  = data.get('old', '')
    new  = data.get('new', '')
    if u.role != 'admin' and not u.check_password(old):
        return jsonify(ok=False, error='Current password incorrect'), 400
    if not new or len(new) < 4:
        return jsonify(ok=False, error='Password must be at least 4 characters'), 400
    u.set_password(new)
    db.session.commit()
    return jsonify(ok=True)

# ─────────────────────────────────────────────
# API — USERS (admin)
# ─────────────────────────────────────────────
@app.route('/api/users')
@login_required
def api_users():
    u = current_user()
    if u.role == 'admin':
        users = User.query.filter_by(is_active=True).order_by(User.role.desc(), User.sort_order, User.name).all()
    else:
        users = User.query.filter_by(is_active=True, role='employee').order_by(User.sort_order, User.name).all()
    return jsonify([x.to_dict() for x in users])

@app.route('/api/users', methods=['POST'])
@admin_required
def api_add_user():
    d = request.get_json()
    if not d.get('name') or not d.get('username'):
        return jsonify(ok=False, error='Name and username required'), 400
    if User.query.filter(db.func.lower(User.username) == d['username'].strip().lower()).first():
        return jsonify(ok=False, error='Username already exists'), 400
    idx = User.query.filter_by(role='employee').count()
    u   = User(
        name=d['name'].strip(), username=d['username'].strip().lower(),
        email=d.get('email','').strip(), role='employee',
        department=d.get('department',''), location=d.get('location',''),
        office=d.get('office',''), phone=d.get('phone',''),
        color=COLORS[idx % len(COLORS)],
        vac_days=int(d.get('vac_days', 21)), sick_days=int(d.get('sick_days', 14)),
        shift_pattern=idx % len(SHIFT_PATTERNS)
    )
    u.set_password(d.get('password', ''))
    db.session.add(u); db.session.commit()
    now = datetime.utcnow()
    ensure_schedule(u, now.year, now.month)
    return jsonify(ok=True, user=u.to_dict())

@app.route('/api/users/<int:uid>', methods=['PUT'])
@admin_required
def api_edit_user(uid):
    u = db.session.get(User, uid)
    if not u: return jsonify(ok=False, error='Not found'), 404
    d = request.get_json()
    for f in ('name','email','department','location','office','phone'):
        if f in d: setattr(u, f, d[f])
    if 'vac_days'   in d: u.vac_days   = int(d['vac_days'])
    if 'sick_days'  in d: u.sick_days  = int(d['sick_days'])
    if 'day_hours'  in d: u.day_hours  = float(d['day_hours'])
    if 'night_hours'in d: u.night_hours= float(d['night_hours'])
    if 'ntfy_topic' in d: u.ntfy_topic = d['ntfy_topic']
    db.session.commit()
    return jsonify(ok=True, user=u.to_dict())

@app.route('/api/users/<int:uid>', methods=['DELETE'])
@admin_required
def api_delete_user(uid):
    u = db.session.get(User, uid)
    if not u or u.role == 'admin': return jsonify(ok=False, error='Cannot delete'), 400
    db.session.delete(u); db.session.commit()
    return jsonify(ok=True)

@app.route('/api/users/<int:uid>/password', methods=['POST'])
@admin_required
def api_set_password(uid):
    u  = db.session.get(User, uid)
    if not u: return jsonify(ok=False, error='Not found'), 404
    pw = (request.get_json() or {}).get('password', '')
    if len(pw) < 4: return jsonify(ok=False, error='Minimum 4 characters'), 400
    u.set_password(pw); db.session.commit()
    return jsonify(ok=True)

@app.route('/api/users/reorder', methods=['POST'])
@admin_required
def api_reorder_users():
    """Reorder employees: body = {"order": [id1, id2, id3, ...]}"""
    ids = (request.get_json() or {}).get('order', [])
    for idx, uid in enumerate(ids):
        u = User.query.get(uid)
        if u and u.role == 'employee':
            u.sort_order = idx
    db.session.commit()
    return jsonify(ok=True)

# ─────────────────────────────────────────────
# API — SCHEDULE
# ─────────────────────────────────────────────
@app.route('/api/schedule/<int:year>/<int:month>')
@login_required
def api_schedule(year, month):
    u = current_user()
    employees = User.query.filter_by(is_active=True, role='employee').order_by(User.sort_order, User.name).all()
    # ensure schedules exist for all employees
    for emp in employees:
        ensure_schedule(emp, year, month)
    days = calendar.monthrange(year, month)[1]
    today = date.today()
    result = []
    for emp in employees:
        row = dict(user=emp.to_dict(), shifts={}, hours={})
        rows = Schedule.query.filter_by(user_id=emp.id, year=year, month=month).all()
        for s in rows:
            row['shifts'][str(s.day)] = s.shift_code
            if s.hours is not None:
                row['hours'][str(s.day)] = s.hours
        # empty days stay as '' (blank) — not auto-filled with DOF
        result.append(row)
    return jsonify(year=year, month=month, days=days,
                   first_weekday=date(year, month, 1).weekday(),
                   today=today.day if today.year==year and today.month==month else -1,
                   employees=result)

@app.route('/api/schedule', methods=['POST'])
@admin_required
def api_update_shift():
    d    = request.get_json()
    uid  = d.get('user_id')
    year = int(d.get('year'))
    month= int(d.get('month'))
    day  = int(d.get('day'))
    code  = d.get('shift_code','DOF')
    hrs   = d.get('hours')
    # Special: BLANK = delete the entry (show empty cell)
    if code == 'BLANK':
        s = Schedule.query.filter_by(user_id=uid, year=year, month=month, day=day).first()
        if s: db.session.delete(s)
        db.session.commit()
        return jsonify(ok=True)
    if code not in SHIFTS: return jsonify(ok=False, error='Invalid shift'), 400
    s = Schedule.query.filter_by(user_id=uid, year=year, month=month, day=day).first()
    if s:
        s.shift_code = code
        if hrs is not None: s.hours = float(hrs) if hrs != '' else None
    else:
        db.session.add(Schedule(user_id=uid, year=year, month=month, day=day, shift_code=code,
                                hours=float(hrs) if hrs not in (None,'') else None))
    db.session.commit()
    return jsonify(ok=True)

@app.route('/api/schedule/clear-month', methods=['POST'])
@admin_required
def api_clear_month():
    """Clear ALL schedule entries for a given month"""
    d     = request.get_json()
    year  = int(d.get('year'))
    month = int(d.get('month'))
    uid   = d.get('user_id')   # optional: if set, clear only that employee
    q = Schedule.query.filter_by(year=year, month=month)
    if uid: q = q.filter_by(user_id=uid)
    deleted = q.delete()
    db.session.commit()
    return jsonify(ok=True, deleted=deleted)

# ─────────────────────────────────────────────
# API — EMPLOYEE SELF-EDIT SCHEDULE
# ─────────────────────────────────────────────
@app.route('/api/my-schedule', methods=['POST'])
@login_required
def api_my_schedule_update():
    u = current_user()
    if AppSettings.get('schedule_locked', 'false') == 'true' and u.role != 'admin':
        return jsonify(ok=False, error='Schedule editing is currently locked by admin'), 403
    d    = request.get_json()
    year = int(d.get('year'))
    month= int(d.get('month'))
    day  = int(d.get('day'))
    code = d.get('shift_code', 'DOF')
    if code not in SHIFTS: return jsonify(ok=False, error='Invalid shift'), 400
    s = Schedule.query.filter_by(user_id=u.id, year=year, month=month, day=day).first()
    if s:
        s.shift_code = code
    else:
        db.session.add(Schedule(user_id=u.id, year=year, month=month, day=day, shift_code=code))
    db.session.commit()
    return jsonify(ok=True)

# ─────────────────────────────────────────────
# API — EXCEL IMPORT
# ─────────────────────────────────────────────
EXCEL_MAP = {
    # Day
    'д': 'D', '12 д': 'D', '12д': 'D', '12 д.': 'D', 'd': 'D', '12d': 'D', '12 d': 'D',
    # Night
    'н': 'N', '12 н': 'N', '12н': 'N', '12 н.': 'N',
    # Day Senior — all Cyrillic/Latin/no-space combos
    '12д сс': 'DSS', '12 д сс': 'DSS', 'д сс': 'DSS',
    '12дсс': 'DSS', 'дсс': 'DSS',
    '12д cc': 'DSS', '12 д cc': 'DSS', 'д cc': 'DSS',
    '12дcc': 'DSS', 'дcc': 'DSS',
    '12д сc': 'DSS', '12д cс': 'DSS',
    '12д ss': 'DSS', '12 д ss': 'DSS', 'д ss': 'DSS',
    '12дss': 'DSS', 'дss': 'DSS',
    # Night Senior — all Cyrillic/Latin/no-space combos
    '12н сс': 'NSS', '12 н сс': 'NSS', 'н сс': 'NSS',
    '12нсс': 'NSS', 'нсс': 'NSS',
    '12н cc': 'NSS', '12 н cc': 'NSS', 'н cc': 'NSS',
    '12нcc': 'NSS', 'нcc': 'NSS',
    '12н сc': 'NSS', '12н cс': 'NSS',
    '12н ss': 'NSS', '12 н ss': 'NSS', 'н ss': 'NSS',
    '12нss': 'NSS', 'нss': 'NSS',
    # Day Trainee
    '12д/с': 'DS', '12 д/с': 'DS', 'д/с': 'DS', '12л/с': 'DS', '12 л/с': 'DS',
    '12д/c': 'DS', 'д/c': 'DS', '12д/s': 'DS',
    # Night Trainee
    '12н/с': 'NS', '12 н/с': 'NS', 'н/с': 'NS',
    '12н/c': 'NS', 'н/c': 'NS', '12н/s': 'NS',
    # Sick
    'б/л': 'BL', 'бл': 'BL', 'б/л.': 'BL',
    # Vacation paid
    'о/о': 'OO', 'оо': 'OO', 'о/о.': 'OO', 'o/o': 'OO',
    # Vacation unpaid / personal
    'о/с': 'OS', 'ос': 'OS', 'o/c': 'OS', 'o/с': 'OS', 'о/c': 'OS',
    # Day Off
    'д/оф': 'DOF', 'доф': 'DOF', 'д/оф.': 'DOF',
    'д/оф ': 'DOF', 'д/оф.': 'DOF',
}

@app.route('/api/schedule/import', methods=['POST'])
@admin_required
def api_import_excel():
    try:
        import openpyxl
    except ImportError:
        return jsonify(ok=False, error='openpyxl not installed'), 500
    f = request.files.get('file')
    if not f: return jsonify(ok=False, error='No file'), 400
    try:
        import calendar as _cal, re as _re
        from datetime import datetime as _dt, date as _date

        wb  = openpyxl.load_workbook(f, data_only=True)
        ws  = wb.active
        year        = int(request.form.get('year',  datetime.utcnow().year))
        month       = int(request.form.get('month', datetime.utcnow().month))
        clear_first = request.form.get('clear_first', '0') == '1'
        days_in_month = _cal.monthrange(year, month)[1]

        # ── Read all rows ──
        all_rows = list(ws.iter_rows())
        if len(all_rows) < 2:
            return jsonify(ok=False, error='File too short')

        SKIP_WORDS = {'mon','tue','wed','thu','fri','sat','sun',
                      'пн','вт','ср','чт','пт','сб','вс',
                      'monday','tuesday','wednesday','thursday','friday','saturday','sunday'}

        def clean_str(val):
            """Convert cell value to clean lowercase string."""
            if val is None: return ''
            s = str(val).strip().lower()
            # normalize unicode: replace NBSP and other whitespace
            s = s.replace('\xa0', ' ').replace('\u200b', '')
            s = _re.sub(r'\s+', ' ', s).strip()
            return s

        def norm(raw):
            if raw is None: return None
            v = clean_str(raw)
            if not v: return None
            # 1. direct lookup
            c = EXCEL_MAP.get(v)
            if c: return c
            # 2. replace ALL Latin look-alikes → Cyrillic
            v2 = (v.replace('o','о').replace('c','с').replace('a','а')
                   .replace('e','е').replace('x','х').replace('p','р')
                   .replace('h','н').replace('b','в').replace('m','м'))
            c = EXCEL_MAP.get(v2)
            if c: return c
            # 3. strip leading "12 " or "12" prefix and retry
            v3 = _re.sub(r'^\d+\s*', '', v2).strip()
            c = EXCEL_MAP.get(v3)
            if c: return c
            v4 = _re.sub(r'^\d+\s*', '', v).strip()
            c = EXCEL_MAP.get(v4)
            if c: return c
            # 4. Pattern-based matching (handles any encoding / spacing combo)
            vp = _re.sub(r'\d+', '', v2).strip()
            vp = _re.sub(r'\s+', ' ', vp).strip()
            is_senior  = bool(_re.search(r'[сc][сc]|ss', vp))
            is_trainee = bool(_re.search(r'[/\\][сcs]', vp))
            is_dof     = bool(_re.search(r'[оo][фf]', vp))
            is_day     = bool(_re.match(r'д', vp))
            is_night   = bool(_re.match(r'н', vp))
            if is_dof:   return 'DOF'
            if is_senior:
                if is_day:   return 'DSS'
                if is_night: return 'NSS'
            if is_trainee:
                if is_day:   return 'DS'
                if is_night: return 'NS'
            if _re.match(r'^д\s*$', vp): return 'D'
            if _re.match(r'^н\s*$', vp): return 'N'
            return None

        # ── Auto-detect col→day mapping from header rows ──
        # Find the row that has sequential day numbers (1,2,3...N)
        col_to_day = {}
        data_start_row = 2   # default: skip 2 header rows
        for ri, row in enumerate(all_rows[:5]):  # check first 5 rows
            day_cols = {}
            for ci, cell in enumerate(row):
                val = cell.value
                try:
                    if val is None: continue
                    if hasattr(val, 'day'):   # datetime object
                        d = val.day
                    else:
                        sv = str(val).strip()
                        # handle "4/1" → take last part after /
                        sv = sv.split('/')[-1].split('-')[-1].strip()
                        d = int(float(sv))
                    if 1 <= d <= 31:
                        day_cols[ci] = d
                except:
                    pass
            # If we found at least 20 day columns → this is the date header row
            if len(day_cols) >= 20:
                col_to_day = day_cols
                data_start_row = ri + 2  # employee rows start after date row + 1 more header
                break

        # Fallback: col B (index 1) = day 1
        if not col_to_day:
            col_to_day = {i: i for i in range(1, days_in_month + 1)}
            data_start_row = 2

        updated      = 0
        errors       = []
        unrecognized = {}   # {emp_name: {day: raw_val}}
        debug_rows   = []   # first 3 employees raw cells for debugging

        for row in all_rows[data_start_row:]:
            name_raw = row[0].value
            if not name_raw: continue
            name_str = str(name_raw).strip()
            if len(name_str) < 2 or name_str.lower() in SKIP_WORDS: continue
            # skip rows where first cell looks like a number/date
            try:
                float(name_str.replace('/', '').replace('-', ''))
                continue  # it's a number → skip
            except: pass

            # find employee by name
            clean = _re.sub(r'\s*[-–]\s*(ru|рu)\s*\d+.*', '', name_str, flags=_re.IGNORECASE).strip()
            emp = User.query.filter(User.name.ilike(f'%{clean}%'), User.role=='employee').first()
            if not emp:
                for word in [w for w in clean.split() if len(w) > 2]:
                    emp = User.query.filter(User.name.ilike(f'%{word}%'), User.role=='employee').first()
                    if emp: break
            if not emp:
                errors.append(f'Not found: {name_str}')
                continue

            # clear old data first if requested
            if clear_first:
                Schedule.query.filter_by(user_id=emp.id, year=year, month=month).delete()
                db.session.flush()

            # read shifts using col_to_day mapping
            raw_cells = {}
            for col_idx, day_num in col_to_day.items():
                if col_idx >= len(row): continue
                if day_num < 1 or day_num > days_in_month: continue
                cell_val = row[col_idx].value
                raw_cells[str(day_num)] = str(cell_val) if cell_val is not None else ''
                code = norm(cell_val)
                if not code:
                    if cell_val not in (None, '') and str(cell_val).strip():
                        unrecognized.setdefault(emp.name, {})[str(day_num)] = str(cell_val).strip()
                    continue
                s = Schedule.query.filter_by(user_id=emp.id, year=year, month=month, day=day_num).first()
                if s:   s.shift_code = code
                else:   db.session.add(Schedule(user_id=emp.id, year=year, month=month, day=day_num, shift_code=code))
                updated += 1

            if len(debug_rows) < 5:
                debug_rows.append({'name': emp.name, 'cells': raw_cells})

        db.session.commit()
        # Build warning from unrecognized
        warn_parts = []
        for ename, days in list(unrecognized.items())[:3]:
            warn_parts.append(f'{ename}: ' + ', '.join(f'Day{d}="{v}"' for d,v in list(days.items())[:5]))
        warn = ' | '.join(warn_parts) if warn_parts else ''
        return jsonify(ok=True, updated=updated, errors=errors, warning=warn,
                       debug=debug_rows, col_map_size=len(col_to_day),
                       data_start=data_start_row)
    except Exception as e:
        import traceback
        return jsonify(ok=False, error=str(e), trace=traceback.format_exc())

# ─────────────────────────────────────────────
# GSHEET → SCHEDULE IMPORT
# ─────────────────────────────────────────────
# Rows / cells containing these labels are silently skipped during import.
GSHEET_IGNORE_PATTERNS = [
    r'^melbet',
    r'^linebet',
    r'^ru\s*\d+',
    r'^\s*1\s*x\s*bet',
    r'^\s*1x-?bet',
    r'by\s*\d+\s*$',
]

def _gsheet_is_ignored(text):
    if not text:
        return False
    import re as __re
    s = str(text).strip().lower()
    if not s:
        return False
    for pat in GSHEET_IGNORE_PATTERNS:
        if __re.search(pat, s):
            return True
    return False

@app.route('/api/schedule/import-from-gsheet', methods=['POST'])
@admin_required
def api_import_from_gsheet():
    import csv, io, calendar as _cal, re as _re2
    try:
        raw = AppSettings.get('gsheet_url', '')
        sid, gid = _extract_gsheet_ids(raw)
        if not sid:
            return jsonify(ok=False, error='Google Sheet URL not configured. Go to Settings and paste the link first.')

        payload = request.get_json(silent=True) or {}
        year  = int(payload.get('year')  or request.form.get('year')  or datetime.utcnow().year)
        month = int(payload.get('month') or request.form.get('month') or datetime.utcnow().month)
        clear_first = str(payload.get('clear_first', request.form.get('clear_first', '1'))) == '1'
        days_in_month = _cal.monthrange(year, month)[1]

        try:
            csv_text = _fetch_gsheet_csv(sid, gid)
        except RuntimeError as e:
            return jsonify(ok=False, error=str(e)), 502

        all_rows = list(csv.reader(io.StringIO(csv_text)))
        if len(all_rows) < 2:
            return jsonify(ok=False, error='Sheet is empty or too short')

        SKIP_WORDS = {'mon','tue','wed','thu','fri','sat','sun',
                      'пн','вт','ср','чт','пт','сб','вс',
                      'monday','tuesday','wednesday','thursday','friday','saturday','sunday',
                      'employee','name','имя','сотрудник'}

        def clean_str(val):
            if val is None:
                return ''
            s = str(val).strip().lower()
            s = s.replace('\xa0', ' ').replace('\u200b', '')
            s = _re2.sub(r'\s+', ' ', s).strip()
            return s

        def norm(raw):
            """Convert a raw cell into a schedule code or None."""
            if raw is None:
                return None
            v = clean_str(raw)
            if not v:
                return None
            # Silently ignore brand labels appearing as shift cells
            if _gsheet_is_ignored(v):
                return None
            c = EXCEL_MAP.get(v)
            if c:
                return c
            v2 = (v.replace('o','о').replace('c','с').replace('a','а')
                   .replace('e','е').replace('x','х').replace('p','р')
                   .replace('h','н').replace('b','в').replace('m','м'))
            c = EXCEL_MAP.get(v2)
            if c:
                return c
            v3 = _re2.sub(r'^\d+\s*', '', v2).strip()
            c = EXCEL_MAP.get(v3)
            if c:
                return c
            v4 = _re2.sub(r'^\d+\s*', '', v).strip()
            c = EXCEL_MAP.get(v4)
            if c:
                return c
            vp = _re2.sub(r'\d+', '', v2).strip()
            vp = _re2.sub(r'\s+', ' ', vp).strip()
            is_senior  = bool(_re2.search(r'[сc][сc]|ss', vp))
            is_trainee = bool(_re2.search(r'[/\\][сcs]', vp))
            is_dof     = bool(_re2.search(r'[оo][фf]', vp))
            is_day     = bool(_re2.match(r'д', vp))
            is_night   = bool(_re2.match(r'н', vp))
            if is_dof:
                return 'DOF'
            if is_senior:
                if is_day:   return 'DSS'
                if is_night: return 'NSS'
            if is_trainee:
                if is_day:   return 'DS'
                if is_night: return 'NS'
            if _re2.match(r'^д\s*$', vp): return 'D'
            if _re2.match(r'^н\s*$', vp): return 'N'
            return None

        # ── Auto-detect day-header row (a row containing 1..31 sequential ints) ──
        col_to_day = {}
        data_start_row = 2
        for ri, row in enumerate(all_rows[:8]):
            day_cols = {}
            for ci, cell in enumerate(row):
                if cell is None:
                    continue
                sv = str(cell).strip()
                if not sv:
                    continue
                sv = sv.split('/')[-1].split('-')[-1].strip()
                try:
                    d = int(float(sv))
                    if 1 <= d <= 31:
                        day_cols[ci] = d
                except Exception:
                    pass
            if len(day_cols) >= 20:
                col_to_day = day_cols
                data_start_row = ri + 1
                break

        if not col_to_day:
            # Fallback — if we cannot detect a date row, assume col B..AF map to days 1..31
            col_to_day = {i: i for i in range(1, min(32, days_in_month + 1))}

        updated       = 0
        skipped_label = []
        not_found     = []
        matched_emps  = []

        for row in all_rows[data_start_row:]:
            if not row:
                continue
            name_raw = row[0] if len(row) > 0 else ''
            if not name_raw:
                continue
            name_str = str(name_raw).strip()
            if not name_str:
                continue
            low = name_str.lower()
            if len(name_str) < 2 or low in SKIP_WORDS:
                continue
            # skip rows that are pure numbers/dates
            try:
                float(name_str.replace('/', '').replace('-', ''))
                continue
            except Exception:
                pass
            # skip brand / category rows (Melbet 8, linebet, Ru 100, 8 by 4, ...)
            if _gsheet_is_ignored(name_str):
                skipped_label.append(name_str)
                continue

            # clean name: drop trailing " - Ru 100", "-linebet", etc.
            clean = name_str
            clean = _re2.sub(r'[-–]\s*(ru|рu)\s*\d+.*', '', clean, flags=_re2.IGNORECASE).strip()
            clean = _re2.sub(r'[-–]\s*(linebet|melbet|1xbet).*', '', clean, flags=_re2.IGNORECASE).strip()
            clean = _re2.sub(r'\s*[-–]\s*$', '', clean).strip()   # trailing dash

            emp = User.query.filter(User.name.ilike(f'%{clean}%'), User.role=='employee').first()
            if not emp:
                for word in [w for w in clean.split() if len(w) > 2]:
                    emp = User.query.filter(User.name.ilike(f'%{word}%'), User.role=='employee').first()
                    if emp:
                        break
            if not emp:
                not_found.append(name_str)
                continue

            if clear_first:
                Schedule.query.filter_by(user_id=emp.id, year=year, month=month).delete()
                db.session.flush()

            matched_emps.append(emp.name)

            for col_idx, day_num in col_to_day.items():
                if col_idx >= len(row):
                    continue
                if day_num < 1 or day_num > days_in_month:
                    continue
                cell_val = row[col_idx]
                code = norm(cell_val)
                if not code:
                    continue
                s = Schedule.query.filter_by(user_id=emp.id, year=year, month=month, day=day_num).first()
                if s:
                    s.shift_code = code
                else:
                    db.session.add(Schedule(user_id=emp.id, year=year, month=month, day=day_num, shift_code=code))
                updated += 1

        db.session.commit()

        # Deduplicate and limit
        matched_emps = sorted(set(matched_emps))
        not_found    = sorted(set(not_found))
        skipped_label = sorted(set(skipped_label))

        return jsonify(
            ok         = True,
            updated    = updated,
            matched    = matched_emps,
            not_found  = not_found,
            skipped    = skipped_label,
            col_map_size = len(col_to_day),
            data_start = data_start_row,
            year       = year,
            month      = month,
        )
    except Exception as e:
        import traceback
        db.session.rollback()
        return jsonify(ok=False, error=str(e), trace=traceback.format_exc()[:800]), 500

# ─────────────────────────────────────────────
# API — REQUESTS
# ─────────────────────────────────────────────
@app.route('/api/requests')
@login_required
def api_requests():
    u      = current_user()
    status = request.args.get('status', 'all')
    rtype  = request.args.get('type', 'all')
    q = Request.query
    if u.role != 'admin':
        q = q.filter_by(user_id=u.id)
    if status != 'all':
        q = q.filter_by(status=status)
    if rtype != 'all':
        q = q.filter_by(type=rtype)
    reqs = q.order_by(Request.created_at.desc()).all()
    return jsonify([r.to_dict() for r in reqs])

@app.route('/api/requests/leave', methods=['POST'])
@login_required
def api_submit_leave():
    u    = current_user()
    d    = request.get_json()
    lt   = d.get('leave_type','annual')
    sd   = date.fromisoformat(d['start_date'])
    ed   = date.fromisoformat(d['end_date'])
    days = (ed - sd).days + 1
    if days < 1: return jsonify(ok=False, error='Invalid dates'), 400
    req  = Request(type='leave', user_id=u.id, leave_type=lt,
                   start_date=sd, end_date=ed, days_count=days,
                   reason=d.get('reason',''))
    db.session.add(req); db.session.commit()
    notify_admin_new_request(u, req)
    return jsonify(ok=True, request=req.to_dict())

@app.route('/api/requests/swap', methods=['POST'])
@login_required
def api_submit_swap():
    u    = current_user()
    d    = request.get_json()
    tid  = d.get('target_user_id')
    tu   = db.session.get(User, tid) if tid else None
    if not tu: return jsonify(ok=False, error='Target employee not found'), 400
    sd   = date.fromisoformat(d['swap_date'])
    year, month, day = sd.year, sd.month, sd.day
    ensure_schedule(u, year, month); ensure_schedule(tu, year, month)
    us   = Schedule.query.filter_by(user_id=u.id,  year=year, month=month, day=day).first()
    ts   = Schedule.query.filter_by(user_id=tu.id, year=year, month=month, day=day).first()
    req  = Request(type='swap', user_id=u.id, target_user_id=tid,
                   swap_date=sd,
                   user_shift=us.shift_code if us else 'DOF',
                   target_shift=ts.shift_code if ts else 'DOF',
                   reason=d.get('reason',''))
    db.session.add(req); db.session.commit()
    notify_admin_new_request(u, req)
    return jsonify(ok=True, request=req.to_dict())

@app.route('/api/requests/draft', methods=['POST'])
@login_required
def api_submit_draft():
    u    = current_user()
    d    = request.get_json()
    dd   = date.fromisoformat(d['draft_date'])
    code = d.get('proposed_shift','DOF')
    if code not in SHIFTS: return jsonify(ok=False, error='Invalid shift'), 400
    ensure_schedule(u, dd.year, dd.month)
    cur  = Schedule.query.filter_by(user_id=u.id, year=dd.year, month=dd.month, day=dd.day).first()
    # check no pending draft for same date
    dup  = Request.query.filter_by(type='draft', user_id=u.id, draft_date=dd, status='pending').first()
    if dup: return jsonify(ok=False, error='A pending proposal already exists for this date'), 400
    req  = Request(type='draft', user_id=u.id, draft_date=dd,
                   proposed_shift=code, current_shift_code=cur.shift_code if cur else 'DOF',
                   reason=d.get('reason',''))
    db.session.add(req); db.session.commit()
    notify_admin_new_request(u, req)
    return jsonify(ok=True, request=req.to_dict())

@app.route('/api/requests/<int:rid>/approve', methods=['POST'])
@admin_required
def api_approve(rid):
    req  = db.session.get(Request, rid)
    if not req or req.status != 'pending':
        return jsonify(ok=False, error='Request not found or already processed'), 400
    note = (request.get_json() or {}).get('note','')
    req.status     = 'approved'
    req.admin_note = note
    req.updated_at = datetime.utcnow()
    emp  = db.session.get(User, req.user_id)
    # apply schedule changes
    if req.type == 'leave':
        d = req.start_date
        while d <= req.end_date:
            code = 'BL' if req.leave_type == 'sick' else ('OS' if req.leave_type == 'personal' else 'OO')
            s = Schedule.query.filter_by(user_id=emp.id, year=d.year, month=d.month, day=d.day).first()
            if s: s.shift_code = code
            else: db.session.add(Schedule(user_id=emp.id, year=d.year, month=d.month, day=d.day, shift_code=code))
            d += timedelta(days=1)
    elif req.type == 'swap':
        tu   = db.session.get(User, req.target_user_id)
        sd   = req.swap_date
        us   = Schedule.query.filter_by(user_id=emp.id, year=sd.year, month=sd.month, day=sd.day).first()
        ts   = Schedule.query.filter_by(user_id=tu.id, year=sd.year, month=sd.month, day=sd.day).first()
        if us and ts:
            us.shift_code, ts.shift_code = req.target_shift, req.user_shift
        # notify target too
        add_notification(tu.id, 'approved', f'✅ Shift swap on {req.swap_date} with {emp.name} was APPROVED.')
    elif req.type == 'draft':
        dd = req.draft_date
        s  = Schedule.query.filter_by(user_id=emp.id, year=dd.year, month=dd.month, day=dd.day).first()
        if s: s.shift_code = req.proposed_shift
        else: db.session.add(Schedule(user_id=emp.id, year=dd.year, month=dd.month, day=dd.day, shift_code=req.proposed_shift))
    db.session.commit()
    notify_and_email(emp, req, note)
    return jsonify(ok=True)

@app.route('/api/requests/<int:rid>/reject', methods=['POST'])
@admin_required
def api_reject(rid):
    req  = db.session.get(Request, rid)
    if not req or req.status != 'pending':
        return jsonify(ok=False, error='Request not found or already processed'), 400
    note = (request.get_json() or {}).get('note','')
    req.status     = 'rejected'
    req.admin_note = note
    req.updated_at = datetime.utcnow()
    db.session.commit()
    notify_and_email(db.session.get(User, req.user_id), req, note)
    return jsonify(ok=True)

# ─────────────────────────────────────────────
# API — NOTIFICATIONS
# ─────────────────────────────────────────────
@app.route('/api/notifications')
@login_required
def api_notifications():
    u     = current_user()
    notifs= Notification.query.filter_by(user_id=u.id).order_by(Notification.created_at.desc()).limit(50).all()
    unread= Notification.query.filter_by(user_id=u.id, is_read=False).count()
    return jsonify(notifications=[n.to_dict() for n in notifs], unread=unread)

@app.route('/api/notifications/read', methods=['POST'])
@login_required
def api_mark_read():
    u = current_user()
    Notification.query.filter_by(user_id=u.id, is_read=False).update({'is_read': True})
    db.session.commit()
    return jsonify(ok=True)

# ─────────────────────────────────────────────
# API — SETTINGS (email + shifts)
# ─────────────────────────────────────────────
@app.route('/api/settings', methods=['GET'])
@admin_required
def api_get_settings():
    return jsonify(
        smtp_server  = AppSettings.get('smtp_server', 'smtp.gmail.com'),
        smtp_port    = AppSettings.get('smtp_port',   '587'),
        smtp_user    = AppSettings.get('smtp_user',   ''),
        smtp_pass    = AppSettings.get('smtp_pass',   ''),
        from_name    = AppSettings.get('from_name',   'Staff Scheduler'),
        email_enabled= AppSettings.get('email_enabled','false'),
        day_hours    = AppSettings.get('day_hours',   '12'),
        night_hours  = AppSettings.get('night_hours', '12'),
        schedule_locked = AppSettings.get('schedule_locked', 'false'),
        ntfy_topic   = AppSettings.get('ntfy_topic',  ''),
        gsheet_url   = AppSettings.get('gsheet_url',  ''),
    )

@app.route('/api/settings', methods=['POST'])
@admin_required
def api_save_settings():
    d = request.get_json()
    for key in ('smtp_server','smtp_port','smtp_user','smtp_pass','from_name',
                'email_enabled','day_hours','night_hours','schedule_locked','ntfy_topic',
                'gsheet_url'):
        if key in d:
            AppSettings.put(key, d[key])
    return jsonify(ok=True)

# ─────────────────────────────────────────────
# GOOGLE SHEET LIVE READ
# ─────────────────────────────────────────────
# Words/brands to hide or replace in the sheet before showing to users.
GSHEET_BRAND_REPLACEMENTS = [
    (r'\bMELBET\s*1\b', 'TEAM B'),
    (r'\bMELBET\s*2\b', 'TEAM C'),
    (r'\bMELBET\s*3\b', 'TEAM D'),
    (r'\bMELBET\b',      'TEAM B'),
    (r'\b1\s*X\s*BET\b','TEAM A'),
    (r'\b1XBET\b',        'TEAM A'),
]

def _extract_gsheet_ids(url):
    import re
    if not url:
        return None, None
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    if not m:
        return None, None
    sid = m.group(1)
    gid_m = re.search(r'[?&#]gid=(\d+)', url)
    gid = gid_m.group(1) if gid_m else '0'
    return sid, gid

def _sanitize_cell(text):
    import re
    if not text:
        return text
    s = str(text)
    for pattern, repl in GSHEET_BRAND_REPLACEMENTS:
        s = re.sub(pattern, repl, s, flags=re.IGNORECASE)
    return s

def _fetch_gsheet_csv(sheet_id, gid):
    import urllib.request as _ur
    import urllib.error as _ue
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'
    req = _ur.Request(url, headers={'User-Agent': 'Mozilla/5.0 StaffScheduler'})
    try:
        with _ur.urlopen(req, timeout=15) as resp:
            data = resp.read()
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError:
            return data.decode('cp1252', errors='replace')
    except _ue.HTTPError as e:
        raise RuntimeError(f'Google returned {e.code}. Make sure the sheet is shared as "Anyone with the link can view".')
    except Exception as e:
        raise RuntimeError(f'Could not load sheet: {e}')

@app.route('/api/gsheet')
@login_required
def api_gsheet_info():
    raw = AppSettings.get('gsheet_url', '')
    sid, gid = _extract_gsheet_ids(raw)
    return jsonify(
        raw_url    = raw,
        configured = bool(sid),
        sheet_id   = sid or '',
        gid        = gid or '',
    )

@app.route('/api/gsheet/data')
@login_required
def api_gsheet_data():
    import csv, io
    raw = AppSettings.get('gsheet_url', '')
    sid, gid = _extract_gsheet_ids(raw)
    if not sid:
        return jsonify(ok=False, error='No Google Sheet URL configured. Ask the admin to add one in Settings.'), 400
    try:
        csv_text = _fetch_gsheet_csv(sid, gid)
    except RuntimeError as e:
        return jsonify(ok=False, error=str(e)), 502

    reader = csv.reader(io.StringIO(csv_text))
    rows = []
    for row in reader:
        cleaned = [_sanitize_cell(c) for c in row]
        if any((c or '').strip() for c in cleaned):
            rows.append(cleaned)

    max_non_empty = 0
    for r in rows:
        last = 0
        for i, c in enumerate(r):
            if (c or '').strip():
                last = i + 1
        if last > max_non_empty:
            max_non_empty = last
    rows = [r[:max_non_empty] for r in rows]

    return jsonify(ok=True, rows=rows, count=len(rows))

@app.route('/api/settings/test-push', methods=['POST'])
@admin_required
def api_test_push():
    topic = AppSettings.get('ntfy_topic', '') or os.environ.get('NTFY_TOPIC', '')
    if not topic:
        return jsonify(ok=False, error='No ntfy topic set')
    try:
        import urllib.request as _ur
        url = f'https://ntfy.sh/{topic}'
        req = _ur.Request(url, data=b'Staff Scheduler: Test notification!',
                          method='POST',
                          headers={
                              'Title': 'Test Notification',
                              'Priority': '3',
                              'Tags': 'bell'
                          })
        resp = _ur.urlopen(req, timeout=10)
        resp_body = resp.read().decode('utf-8')
        return jsonify(ok=True, message=f'Sent to ntfy.sh/{topic}', response=resp_body)
    except Exception as e:
        return jsonify(ok=False, error=f'ntfy error: {str(e)}')

@app.route('/api/requests/apply-approved', methods=['POST'])
@admin_required
def api_apply_approved_drafts():
    """Apply all approved draft requests to the actual schedule"""
    drafts = Request.query.filter_by(type='draft', status='approved').all()
    applied = 0
    for req in drafts:
        dd = req.draft_date
        s  = Schedule.query.filter_by(user_id=req.user_id, year=dd.year, month=dd.month, day=dd.day).first()
        if s: s.shift_code = req.proposed_shift
        else: db.session.add(Schedule(user_id=req.user_id, year=dd.year, month=dd.month, day=dd.day, shift_code=req.proposed_shift))
        applied += 1
    db.session.commit()
    return jsonify(ok=True, applied=applied)

@app.route('/api/settings/test-email', methods=['POST'])
@admin_required
def api_test_email():
    u = current_user()
    if not u.email:
        return jsonify(ok=False, error='Admin has no email set'), 400
    try:
        smtp_srv  = AppSettings.get('smtp_server', 'smtp.gmail.com')
        smtp_port = int(AppSettings.get('smtp_port', '587'))
        smtp_user = AppSettings.get('smtp_user', '')
        smtp_pass = AppSettings.get('smtp_pass', '')
        from_name = AppSettings.get('from_name', 'Staff Scheduler')
        enabled   = AppSettings.get('email_enabled', 'false') == 'true'
        if not enabled:
            return jsonify(ok=False, error='Email is disabled — enable it first')
        if not smtp_user:
            return jsonify(ok=False, error='No sender email set')
        if not smtp_pass:
            return jsonify(ok=False, error='No App Password set')
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'Test — Staff Scheduler'
        msg['From']    = f'{from_name} <{smtp_user}>'
        msg['To']      = u.email
        msg.attach(MIMEText('<p>✅ Email configuration is working!</p>', 'html', 'utf-8'))
        with smtplib.SMTP(smtp_srv, smtp_port) as srv:
            srv.ehlo(); srv.starttls(); srv.ehlo()
            srv.login(smtp_user, smtp_pass)
            srv.sendmail(smtp_user, u.email, msg.as_string())
        return jsonify(ok=True, message=f'Email sent to {u.email}')
    except Exception as e:
        return jsonify(ok=False, error=str(e))

# ─────────────────────────────────────────────
# API — STATS
# ─────────────────────────────────────────────
@app.route('/api/stats')
@admin_required
def api_stats():
    total    = User.query.filter_by(role='employee', is_active=True).count()
    pending  = Request.query.filter_by(status='pending').count()
    approved = Request.query.filter_by(status='approved').count()
    rejected = Request.query.filter_by(status='rejected').count()
    recent   = Request.query.filter_by(status='pending').order_by(Request.created_at.desc()).limit(5).all()
    return jsonify(total=total, pending=pending, approved=approved, rejected=rejected,
                   recent=[r.to_dict() for r in recent])

# ─────────────────────────────────────────────
# DB SEED
# ─────────────────────────────────────────────
def seed_db():
    if User.query.first():
        return
    # Seed email settings
    defaults = {
        'smtp_server':   'smtp.gmail.com',
        'smtp_port':     '587',
        'smtp_user':     'davidgerges1510@gmail.com',
        'smtp_pass':     'ksux zkdn ulqs wlfu',
        'from_name':     'David Gerges',
        'email_enabled': 'true',
        'day_hours':     '12',
        'night_hours':   '12',
        'ntfy_topic':    'staff-david-2026',
    }
    for k, v in defaults.items():
        AppSettings.put(k, v)

    admin = User(name='David Gerges', username='admin', role='admin',
                 email='davidgerges1510@gmail.com', color='#1a9e9e')
    admin.set_password('admin123')
    db.session.add(admin)

    employees = [
        ('Arakelyan Hayk',      'arakelyan'),
        ('Pier Nuri',           'pier'),
        ('Christina Kaprielian','christina'),
        ('Ahmad Nada',          'ahmad'),
        ('Rogeh Akobjian',      'rogeh'),
        ('Jamcosian Sarin',     'jamcosian'),
        ('Mardigian Nanor',     'mardigian'),
        ('Kasbar Mike',         'kasbar'),
        ('Boyajian Nareg',      'boyajian'),
        ('Obaid Ali',           'obaid'),
        ('Khalil Rahaj Bilal',  'khalil'),
        ('Ali Taleb',           'alitaleb'),
    ]
    emp_objs = []
    for i, (name, uname) in enumerate(employees):
        emp = User(name=name, username=uname, email='', role='employee',
                   color=COLORS[i % len(COLORS)], sort_order=i)
        emp.set_password('pass123')
        db.session.add(emp)
        emp_objs.append(emp)
    db.session.commit()

    # Seed November 2026 schedule from Excel data
    SCHEDULE_DATA = {
        'Arakelyan Hayk': {2:'NSS',3:'NSS',4:'DOF',5:'DOF',6:'DSS',7:'DSS',10:'NSS',11:'NSS',15:'DSS',18:'NSS',19:'NSS',22:'DSS',23:'DSS',26:'NSS',27:'NSS',30:'DSS'},
        'Pier Nuri': {1:'D',2:'D',3:'D',4:'D',5:'D',6:'D',7:'OO',8:'OO',9:'OO',10:'OO',11:'OO',12:'OO',13:'OO',14:'OO',15:'OO',16:'N',17:'N',20:'D',21:'D',22:'D',24:'N',25:'N',28:'D',29:'D',30:'D'},
        'Christina Kaprielian': {2:'DOF',3:'N',4:'N',7:'N',8:'N',9:'N',11:'N',14:'DOF',15:'D',16:'DOF',17:'D',19:'N',20:'N',23:'D',24:'D',26:'D',29:'DOF',30:'N'},
        'Ahmad Nada': {2:'D',4:'D',5:'D',6:'D',7:'D',8:'D',10:'D',11:'D',12:'D',13:'D',14:'D',15:'D',17:'D',18:'D',20:'D',21:'D',22:'D',24:'D',25:'D',27:'D',28:'D',29:'D'},
        'Rogeh Akobjian': {1:'BL',2:'BL',3:'BL',4:'BL',5:'D',6:'N',9:'N',13:'N',16:'N',19:'D',20:'N',22:'N',24:'N',26:'D',27:'N',29:'N',30:'N'},
        'Jamcosian Sarin': {1:'D',2:'D',4:'D',7:'D',8:'D',10:'D',11:'N',14:'N',15:'N',18:'D',20:'N',23:'D',24:'D',25:'D',27:'D',29:'D',30:'D'},
        'Mardigian Nanor': {3:'D',4:'D',5:'DOF',6:'D',9:'D',11:'D',12:'D',14:'D',16:'D',17:'D',20:'DOF',21:'D',22:'D',24:'D',25:'D',26:'D',28:'D',31:'D'},
        'Kasbar Mike': {1:'N',4:'N',5:'N',6:'N',7:'N',8:'N',11:'N',12:'N',13:'N',15:'D',16:'N',18:'D',19:'D',20:'N',21:'N',23:'N',24:'N',25:'N',26:'N',28:'N',29:'N',30:'N'},
        'Boyajian Nareg': {1:'N',4:'N',5:'N',8:'D',9:'N',12:'N',13:'N',16:'N',17:'N',20:'N',21:'N',24:'N',25:'N',28:'N',29:'N'},
        'Obaid Ali': {3:'DS',4:'DS',5:'DS',7:'NS',8:'NS',9:'NS',11:'DS',12:'DS',13:'DS',15:'NS',16:'NS',17:'NS',20:'DS',21:'DS',22:'DS',24:'DS',25:'DS',26:'DS',28:'NS',29:'NS'},
        'Khalil Rahaj Bilal': {2:'DS',3:'DS',6:'NS',7:'NS',8:'NS',10:'NS',11:'NS',13:'NS',14:'NS',16:'DS',17:'DS',20:'NS',21:'NS',23:'NS',24:'NS',25:'NS',27:'NS',28:'NS',30:'NS'},
        'Ali Taleb': {14:'DS',15:'DS',18:'NS',19:'NS',22:'DS',23:'DS',26:'NS',27:'NS',30:'DS'},
    }
    for emp in emp_objs:
        sched = SCHEDULE_DATA.get(emp.name.strip(), {})
        for day, shift_code in sched.items():
            try:
                s = Schedule(user_id=emp.id, year=2026, month=11, day=day, shift=shift_code)
                db.session.add(s)
            except Exception:
                pass
    db.session.commit()

# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────
def upgrade_db():
    """Add missing columns to existing database (safe migration)"""
    cols = [
        ('day_hours',   'ALTER TABLE "user" ADD COLUMN day_hours FLOAT DEFAULT 12.0'),
        ('night_hours', 'ALTER TABLE "user" ADD COLUMN night_hours FLOAT DEFAULT 12.0'),
        ('sort_order',  'ALTER TABLE "user" ADD COLUMN sort_order INTEGER DEFAULT 0'),
        ('sch_hours',   'ALTER TABLE schedule ADD COLUMN hours FLOAT'),
        ('ntfy_topic',       'ALTER TABLE "user" ADD COLUMN ntfy_topic VARCHAR(100) DEFAULT \'\''),
        ('telegram_chat_id', 'ALTER TABLE "user" ADD COLUMN telegram_chat_id VARCHAR(50) DEFAULT \'\''),
    ]
    with db.engine.connect() as conn:
        for col, sql in cols:
            try:
                conn.execute(db.text(sql))
                conn.commit()
                print(f"✅ Added column {col}")
            except Exception:
                conn.rollback()

try:
    with app.app_context():
        os.makedirs(os.path.join(basedir, 'instance'), exist_ok=True)
        db.create_all()
        upgrade_db()
        seed_db()
        print("✅ Database initialized successfully")
except Exception as _startup_err:
    import traceback
    print(f"❌ Startup error: {_startup_err}")
    traceback.print_exc()

if __name__ == '__main__':
    import socket
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = '127.0.0.1'
    print(f'\n{"="*50}')
    print(f'  Staff Scheduler System')
    print(f'  Local:   http://127.0.0.1:5000')
    print(f'  Network: http://{local_ip}:5000')
    print(f'  Admin login: username=admin, password=(empty)')
    print(f'{"="*50}\n')
    app.run(host='0.0.0.0', port=5000, debug=False)
