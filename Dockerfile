# Use light version of python environment
FROM alpine:latest

# Install python and make new user which will use the app
RUN apk add --no-cache python3 py3-pip && \
  addgroup -g 1001 -S appuser && adduser -u 1001 -S appuser -G appuser 

# Copy only needed files, set new user as owner
# Avoid installing everything as administrator
USER appuser
COPY --chown=1001:1001 . /home/appuser/app/
WORKDIR /home/appuser/app/
# Install Python dependencies, add .local to PATH, populate database
ENV PATH="/home/appuser/.local/bin:$PATH"
RUN pip install -r requirements.txt && python get_raw_data.py

# Move to the actual app folder and run it
WORKDIR /home/appuser/app/financial/
# Expose the port and run app
EXPOSE 5000
CMD ["uvicorn", "api:APP", "--host", "0.0.0.0", "--port", "5000"]
