from fastapi import FastAPI, HTTPException, Header, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
import logging
from datetime import datetime
import uuid
from minio import Minio
from cassandra.cluster import Cluster

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# FastAPI app
# -----------------------
app = FastAPI(
    title="MS2 - Ajout de Fichiers",
    description="Microservice pour permettre aux enseignants d'ajouter des cours",
    version="1.0.0"
)

# -----------------------
# CORS
# -----------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Fake JWT verification (to replace with Keycloak later)
# -----------------------
def verify_teacher_token(authorization: str = Header(None)):
    """Vérifie que le token appartient à un enseignant"""
    if not authorization:
        raise HTTPException(status_code=401, detail="Token manquant")
    token = authorization.replace("Bearer ", "")
    if token == "fake-token-enseignant":
        return {"valid": True, "user": "prof1", "role": "enseignant"}
    else:
        raise HTTPException(status_code=401, detail="Token invalide")

# -----------------------
# MinIO client
# -----------------------
MINIO_CLIENT = Minio(
    "minio:9000",  # Container name or host
    access_key="MINIO_ACCESS_KEY",
    secret_key="MINIO_SECRET_KEY",
    secure=False
)
BUCKET_NAME = "courses"
if not MINIO_CLIENT.bucket_exists(BUCKET_NAME):
    MINIO_CLIENT.make_bucket(BUCKET_NAME)

# -----------------------
# Cassandra client
# -----------------------
cluster = Cluster(["cassandra"])  # Container name or host
session = cluster.connect()
KEYSPACE = "ent_keyspace"
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class':'SimpleStrategy', 'replication_factor':'1'}}
""")
session.set_keyspace(KEYSPACE)
session.execute("""
    CREATE TABLE IF NOT EXISTS courses (
        id uuid PRIMARY KEY,
        title text,
        description text,
        file_url text,
        teacher text,
        created_at timestamp
    )
""")

# -----------------------
# Routes
# -----------------------
@app.get("/")
async def root():
    """Vérifie que le service tourne"""
    return {"service": "MS2 - Ajout de Fichiers", "status": "OK"}

@app.post("/api/courses")
async def upload_course(
    title: str,
    description: str,
    file: UploadFile = File(...),
    user=Depends(verify_teacher_token)
):
    """Permet à un enseignant d'ajouter un cours avec fichier"""
    teacher = user.get("user")
    file_name = f"{uuid.uuid4()}_{file.filename}"

    # Upload file to MinIO
    MINIO_CLIENT.put_object(
        BUCKET_NAME,
        file_name,
        file.file,
        length=-1,
        part_size=10*1024*1024
    )
    file_url = f"minio://{BUCKET_NAME}/{file_name}"

    # Store metadata in Cassandra
    session.execute(
        "INSERT INTO courses (id, title, description, file_url, teacher, created_at) VALUES (uuid(), %s, %s, %s, %s, %s)",
        (title, description, file_url, teacher, datetime.utcnow())
    )

    logger.info(f"Course '{title}' uploaded by {teacher}")
    return {"message": "Course uploaded successfully", "file_url": file_url}

@app.get("/api/courses")
async def list_courses(user=Depends(verify_teacher_token)):
    """Liste tous les cours (enseignants et étudiants authentifiés)"""
    rows = session.execute("SELECT id, title, description, file_url, teacher, created_at FROM courses")
    return {"courses": [dict(row._asdict()) for row in rows]}
