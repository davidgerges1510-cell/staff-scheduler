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
db = SQLAlchemy(app)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
SHIFTS = {
    'D':   {'name': 'Day',            'icon': '☀️',  'bg': '#c6f6d5', 'color': '#276749'},
    'N':   {'name': 'Night',          'icon': '🌙',  'bg': '#e9d8fd', 'color': '#553c9a'},
    'DS':  {'name': 'Day Trainee',    'icon': '🌤️', 'bg': '#fefcbf', 'color': '#744210'},
    'NS':  {'name': 'Night Trainee',  'icon': '🌛',  'bg': '#e9d8fd', 'color': '#322659'},
    'BL':  {'name': 'Sick Leave',     'icon': '🤒',  'bg': '#fed7d7', 'color': '#9b2c2c'},
    'OO':  {'name': 'Annual Leave',   'icon': '✈️',  'bg': '#bee3f8', 'color': '#2a4365'},
    'DOF': {'name': 'Day Off',        'icon': '😴',  'bg': '#f7fafc', 'color': '#718096'},
    'OS':  {'name': 'Personal Leave', 'icon': '🏠',  'bg': '#fffbeb', 'color': '#744210'},
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
                    initials=self.initials())


class Schedule(db.Model):
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    year       = db.Column(db.Integer, nullable=False)
    month      = db.Column(db.Integer, nullable=False)
    day        = db.Column(db.Integer, nullable=False)
    shift_code = db.Column(db.String(10), default='DOF')
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
    """Auto-generate schedule for a month if not exists."""
    days = calendar.monthrange(year, month)[1]
    existing = {s.day for s in Schedule.query.filter_by(user_id=user.id, year=year, month=month).all()}
    pat = SHIFT_PATTERNS[user.shift_pattern % len(SHIFT_PATTERNS)]
    for d in range(1, days + 1):
        if d not in existing:
            first_day_of_year = date(year, 1, 1)
            day_of_year = (date(year, month, d) - first_day_of_year).days
            shift = pat[day_of_year % 7]
            db.session.add(Schedule(user_id=user.id, year=year, month=month, day=d, shift_code=shift))
    db.session.commit()

def add_notification(user_id, ntype, message):
    db.session.add(Notification(user_id=user_id, type=ntype, message=message))
    db.session.commit()

def send_email_async(to_email, subject, html_body):
    def _send():
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
        except Exception as e:
            print(f'[EMAIL ERROR] {e}')
    threading.Thread(target=_send, daemon=True).start()

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
    send_email_async(emp.email, subj, html)

# ─────────────────────────────────────────────
# AUTH ROUTES
# ─────────────────────────────────────────────
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
        return redirect(url_for('dashboard'))
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
        row = dict(user=emp.to_dict(), shifts={})
        rows = Schedule.query.filter_by(user_id=emp.id, year=year, month=month).all()
        for s in rows:
            row['shifts'][str(s.day)] = s.shift_code
        # fill any missing
        for d in range(1, days+1):
            if str(d) not in row['shifts']:
                row['shifts'][str(d)] = 'DOF'
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
    code = d.get('shift_code','DOF')
    if code not in SHIFTS: return jsonify(ok=False, error='Invalid shift'), 400
    s = Schedule.query.filter_by(user_id=uid, year=year, month=month, day=day).first()
    if s:
        s.shift_code = code
    else:
        db.session.add(Schedule(user_id=uid, year=year, month=month, day=day, shift_code=code))
    db.session.commit()
    return jsonify(ok=True)

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
    'д': 'D', '12 д': 'D', '12д': 'D',
    'н': 'N', '12 н': 'N', '12н': 'N',
    '12д/с': 'DS', '12 д/с': 'DS', 'д/с': 'DS', '12л/с': 'DS',
    '12н/с': 'NS', '12 н/с': 'NS', 'н/с': 'NS', '12 н сс': 'NS', '12н сс': 'NS',
    'б/л': 'BL', 'бл': 'BL',
    'о/о': 'OO', 'оо': 'OO',
    'д/оф': 'DOF', 'доф': 'DOF', 'дof': 'DOF',
    'о/с': 'OS', 'ос': 'OS',
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
        wb = openpyxl.load_workbook(f, data_only=True)
        ws = wb.active
        year  = int(request.form.get('year',  datetime.utcnow().year))
        month = int(request.form.get('month', datetime.utcnow().month))
        import calendar as _cal
        days_in_month = _cal.monthrange(year, month)[1]
        updated = 0
        errors  = []
        for row in ws.iter_rows(min_row=2):
            name_cell = row[0].value
            if not name_cell: continue
            name_str = str(name_cell).strip()
            # Remove extra info like "- Ru 100"
            clean_name = name_str.split('-')[0].strip()
            # Try full name match first
            emp = User.query.filter(User.name.ilike(f'%{clean_name}%'), User.role=='employee').first()
            if not emp:
                # Try matching by individual words (first or last name)
                words = [w for w in clean_name.split() if len(w) > 2]
                for word in words:
                    emp = User.query.filter(User.name.ilike(f'%{word}%'), User.role=='employee').first()
                    if emp:
                        break
            if not emp:
                errors.append(f'Employee not found: {name_str}')
                continue
            for col_idx in range(1, days_in_month + 1):
                if col_idx >= len(row) + 1: break
                cell = row[col_idx]
                val  = str(cell.value or '').strip().lower()
                code = EXCEL_MAP.get(val)
                if not code: continue
                s = Schedule.query.filter_by(user_id=emp.id, year=year, month=month, day=col_idx).first()
                if s:
                    s.shift_code = code
                else:
                    db.session.add(Schedule(user_id=emp.id, year=year, month=month, day=col_idx, shift_code=code))
                updated += 1
        db.session.commit()
        return jsonify(ok=True, updated=updated, errors=errors)
    except Exception as e:
        return jsonify(ok=False, error=str(e))

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
    )

@app.route('/api/settings', methods=['POST'])
@admin_required
def api_save_settings():
    d = request.get_json()
    for key in ('smtp_server','smtp_port','smtp_user','smtp_pass','from_name',
                'email_enabled','day_hours','night_hours','schedule_locked'):
        if key in d:
            AppSettings.put(key, d[key])
    return jsonify(ok=True)

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
    admin = User(name='Admin', username='admin', role='admin',
                 email='', color='#1a9e9e')
    admin.set_password('')
    db.session.add(admin)

    sample = [
        ('Ahmed Al-Salem',   'ahmed',   'ahmed@example.com',  'Operations', 'Site A', 'Office 101', '05012345'),
        ('Sara Al-Zahrani',  'sara',    'sara@example.com',   'Support',    'Site B', 'Office 102', '05076543'),
        ('Khalid Al-Otaibi', 'khalid',  'khalid@example.com', 'Technical',  'Site A', 'Office 103', '05098765'),
    ]
    for i, (name, uname, email, dept, loc, office, phone) in enumerate(sample):
        emp = User(name=name, username=uname, email=email, role='employee',
                   department=dept, location=loc, office=office, phone=phone,
                   color=COLORS[i % len(COLORS)], shift_pattern=i % len(SHIFT_PATTERNS))
        emp.set_password('pass123')
        db.session.add(emp)
    db.session.commit()

    now = datetime.utcnow()
    for emp in User.query.filter_by(role='employee').all():
        ensure_schedule(emp, now.year, now.month)

# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────
def upgrade_db():
    """Add missing columns to existing database (safe migration)"""
    cols = [
        ('day_hours',   'ALTER TABLE "user" ADD COLUMN day_hours FLOAT DEFAULT 12.0'),
        ('night_hours', 'ALTER TABLE "user" ADD COLUMN night_hours FLOAT DEFAULT 12.0'),
        ('sort_order',  'ALTER TABLE "user" ADD COLUMN sort_order INTEGER DEFAULT 0'),
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
