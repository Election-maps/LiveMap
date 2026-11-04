package com.srikakulam.police.tracker;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.os.Build;
import android.os.Bundle;
import android.os.IBinder;
import android.os.PowerManager;
import android.util.Log;

import androidx.core.app.NotificationCompat;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

public class LocationTrackingService extends Service implements LocationListener {

    private static final String TAG = "PoliceLocationService";
    private static final String CHANNEL_ID = "skl_police_duty_channel";
    private static final int NOTIFICATION_ID = 1001;

    public static final String ACTION_START = "ACTION_START";
    public static final String ACTION_STOP = "ACTION_STOP";

    public static final String EXTRA_UNIT_ID = "EXTRA_UNIT_ID";
    public static final String EXTRA_CATEGORY = "EXTRA_CATEGORY";
    public static final String EXTRA_MANDAL = "EXTRA_MANDAL";
    public static final String EXTRA_UNIT_NAME = "EXTRA_UNIT_NAME";
    public static final String EXTRA_OFFICER = "EXTRA_OFFICER";
    public static final String EXTRA_PHONE = "EXTRA_PHONE";
    public static final String EXTRA_ROOM = "EXTRA_ROOM";

    public static boolean isRunning = false;

    private LocationManager locationManager;
    private PowerManager.WakeLock wakeLock;
    private MqttClient mqttClient;

    private String unitId = "";
    private String category = "";
    private String mandal = "";
    private String unitName = "";
    private String officer = "";
    private String phone = "";
    private String roomCode = "SrikakulamPolice";

    private double lastLat = 0;
    private double lastLng = 0;
    private double totalDistanceMeters = 0;
    private int updatesSent = 0;

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();

        locationManager = (LocationManager) getSystemService(Context.LOCATION_SERVICE);

        PowerManager powerManager = (PowerManager) getSystemService(Context.POWER_SERVICE);
        if (powerManager != null) {
            wakeLock = powerManager.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "SrikakulamPolice::TrackingLock");
            wakeLock.acquire(24 * 60 * 60 * 1000L); // 24 hours max
        }

        initMqtt();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    "శ్రీకాకుళం పోలీస్ ఎన్నికల బందోబస్తు",
                    NotificationManager.IMPORTANCE_LOW
            );
            channel.setDescription("నిరంతర బ్యాక్‌గ్రౌండ్ లొకేషన్ ట్రాకింగ్");
            channel.setShowBadge(true);
            NotificationManager manager = getSystemService(NotificationManager.class);
            if (manager != null) {
                manager.createNotificationChannel(channel);
            }
        }
    }

    private void initMqtt() {
        new Thread(() -> {
            try {
                String broker = "tcp://broker.emqx.io:1883";
                String clientId = "skl_native_" + System.currentTimeMillis();
                mqttClient = new MqttClient(broker, clientId, new MemoryPersistence());
                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                options.setConnectionTimeout(10);
                options.setKeepAliveInterval(30);
                options.setAutomaticReconnect(true);
                mqttClient.connect(options);
                Log.d(TAG, "Native MQTT Connected successfully!");
            } catch (Exception e) {
                Log.e(TAG, "MQTT Connection error: " + e.getMessage());
            }
        }).start();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null) {
            String action = intent.getAction();
            if (ACTION_STOP.equals(action)) {
                stopTracking();
                stopSelf();
                return START_NOT_STICKY;
            }

            if (ACTION_START.equals(action)) {
                unitId = intent.getStringExtra(EXTRA_UNIT_ID);
                category = intent.getStringExtra(EXTRA_CATEGORY);
                mandal = intent.getStringExtra(EXTRA_MANDAL);
                unitName = intent.getStringExtra(EXTRA_UNIT_NAME);
                officer = intent.getStringExtra(EXTRA_OFFICER);
                phone = intent.getStringExtra(EXTRA_PHONE);
                String room = intent.getStringExtra(EXTRA_ROOM);
                if (room != null && !room.isEmpty()) roomCode = room;

                saveState();
            }
        }

        if (unitId == null || unitId.isEmpty()) {
            loadSavedState();
        }

        startForeground(NOTIFICATION_ID, buildNotification("సిద్ధంగా ఉంది..."));
        startGpsUpdates();
        isRunning = true;

        return START_STICKY;
    }

    private void saveState() {
        SharedPreferences sp = getSharedPreferences("skl_police_state", MODE_PRIVATE);
        sp.edit()
                .putString("unitId", unitId)
                .putString("category", category)
                .putString("mandal", mandal)
                .putString("unitName", unitName)
                .putString("officer", officer)
                .putString("phone", phone)
                .putString("roomCode", roomCode)
                .apply();
    }

    private void loadSavedState() {
        SharedPreferences sp = getSharedPreferences("skl_police_state", MODE_PRIVATE);
        unitId = sp.getString("unitId", "RM_Default");
        category = sp.getString("category", "Route Mobiles");
        mandal = sp.getString("mandal", "Srikakulam");
        unitName = sp.getString("unitName", "Party");
        officer = sp.getString("officer", "Police Officer");
        phone = sp.getString("phone", "");
        roomCode = sp.getString("roomCode", "SrikakulamPolice");
    }

    private Notification buildNotification(String statusText) {
        Intent openAppIntent = new Intent(this, MainActivity.class);
        PendingIntent pendingOpen = PendingIntent.getActivity(
                this, 0, openAppIntent,
                PendingIntent.FLAG_UPDATE_CURRENT | (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ? PendingIntent.FLAG_IMMUTABLE : 0)
        );

        Intent stopIntent = new Intent(this, LocationTrackingService.class);
        stopIntent.setAction(ACTION_STOP);
        PendingIntent pendingStop = PendingIntent.getService(
                this, 1, stopIntent,
                PendingIntent.FLAG_UPDATE_CURRENT | (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ? PendingIntent.FLAG_IMMUTABLE : 0)
        );

        String title = "🚨 " + mandal + " - " + unitName;
        String content = (officer != null && !officer.isEmpty() ? officer + " • " : "") + statusText;

        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle(title)
                .setContentText(content)
                .setSmallIcon(R.drawable.ic_notification)
                .setContentIntent(pendingOpen)
                .addAction(R.drawable.ic_notification, "ఆపండి (Stop)", pendingStop)
                .setOngoing(true)
                .setPriority(NotificationCompat.PRIORITY_HIGH)
                .build();
    }

    private void startGpsUpdates() {
        try {
            if (locationManager != null) {
                if (locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER)) {
                    locationManager.requestLocationUpdates(
                            LocationManager.GPS_PROVIDER,
                            2500L,
                            1.5f,
                            this
                    );
                }
                if (locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER)) {
                    locationManager.requestLocationUpdates(
                            LocationManager.NETWORK_PROVIDER,
                            3000L,
                            2.0f,
                            this
                    );
                }
            }
        } catch (SecurityException e) {
            Log.e(TAG, "Location permission missing: " + e.getMessage());
        }
    }

    @Override
    public void onLocationChanged(Location location) {
        if (location == null) return;

        double lat = location.getLatitude();
        double lng = location.getLongitude();
        float speed = location.getSpeed();
        float accuracy = location.getAccuracy();

        if (accuracy > 35.0f) return; // filter noisy inaccurate GPS fixes

        boolean isMoving = false;
        if (lastLat != 0 && lastLng != 0) {
            float[] results = new float[1];
            Location.distanceBetween(lastLat, lastLng, lat, lng, results);
            // Ignore stationary GPS noise (< 14 meters when speed is low)
            if (results[0] >= 14.0f || (speed >= 0.8f && results[0] >= 8.0f)) {
                totalDistanceMeters += results[0];
                lastLat = lat;
                lastLng = lng;
                isMoving = true;
            }
        } else {
            lastLat = lat;
            lastLng = lng;
        }
        updatesSent++;

        int kmh = isMoving ? Math.round(speed * 3.6f) : 0;
        String distStr = totalDistanceMeters >= 1000
                ? String.format("%.2f km", totalDistanceMeters / 1000f)
                : Math.round(totalDistanceMeters) + " m";

        String updateInfo = (isMoving ? kmh + " km/h" : "ఆగి ఉన్నారు") + " • " + distStr + " • అప్‌డేట్స్: " + updatesSent;
        NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        if (nm != null) {
            nm.notify(NOTIFICATION_ID, buildNotification(updateInfo));
        }

        publishLocation(lat, lng, isMoving ? speed : 0, accuracy);
    }

    private void publishLocation(double lat, double lng, float speed, float accuracy) {
        new Thread(() -> {
            try {
                if (mqttClient == null || !mqttClient.isConnected()) {
                    initMqtt();
                    Thread.sleep(1000);
                }

                if (mqttClient != null && mqttClient.isConnected()) {
                    JSONObject json = new JSONObject();
                    json.put("type", "location");
                    json.put("userId", "native_" + unitId);
                    json.put("unitId", unitId);
                    json.put("category", category);
                    json.put("mandal", mandal);
                    json.put("unitName", unitName);
                    json.put("officer", officer);
                    json.put("phone", phone);
                    json.put("lat", lat);
                    json.put("lng", lng);
                    json.put("speed", speed);
                    json.put("accuracy", accuracy);
                    json.put("distanceTraveled", Math.round(totalDistanceMeters));
                    json.put("updatesCount", updatesSent);
                    json.put("lastUpdated", System.currentTimeMillis());
                    json.put("status", "live");

                    byte[] payload = json.toString().getBytes("UTF-8");

                    // Retained unit topic
                    String unitTopic = "srikakulam_police_tracker/" + roomCode + "/officer/" + unitId;
                    MqttMessage msgRetained = new MqttMessage(payload);
                    msgRetained.setQos(1);
                    msgRetained.setRetained(true);
                    mqttClient.publish(unitTopic, msgRetained);

                    // General broadcast topic
                    String generalTopic = "srikakulam_police_tracker/" + roomCode + "/locations";
                    MqttMessage msgGeneral = new MqttMessage(payload);
                    msgGeneral.setQos(0);
                    mqttClient.publish(generalTopic, msgGeneral);
                }
            } catch (Exception e) {
                Log.e(TAG, "Failed to publish native MQTT: " + e.getMessage());
            }
        }).start();
    }

    private void stopTracking() {
        isRunning = false;
        try {
            if (locationManager != null) {
                locationManager.removeUpdates(this);
            }
        } catch (Exception ignored) {}

        new Thread(() -> {
            try {
                if (mqttClient != null && mqttClient.isConnected()) {
                    JSONObject json = new JSONObject();
                    json.put("type", "location");
                    json.put("unitId", unitId);
                    json.put("status", "stopped");
                    json.put("lastUpdated", System.currentTimeMillis());

                    String unitTopic = "srikakulam_police_tracker/" + roomCode + "/officer/" + unitId;
                    MqttMessage msg = new MqttMessage(json.toString().getBytes("UTF-8"));
                    msg.setQos(1);
                    msg.setRetained(true);
                    mqttClient.publish(unitTopic, msg);
                    mqttClient.disconnect();
                }
            } catch (Exception ignored) {}
        }).start();

        if (wakeLock != null && wakeLock.isHeld()) {
            wakeLock.release();
        }

        stopForeground(true);
    }

    @Override
    public void onDestroy() {
        stopTracking();
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override public void onStatusChanged(String provider, int status, Bundle extras) {}
    @Override public void onProviderEnabled(String provider) {}
    @Override public void onProviderDisabled(String provider) {}
}
