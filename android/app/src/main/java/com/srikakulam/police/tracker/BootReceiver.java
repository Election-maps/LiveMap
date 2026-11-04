package com.srikakulam.police.tracker;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;

import androidx.core.content.ContextCompat;

public class BootReceiver extends BroadcastReceiver {
    @Override
    public void onReceive(Context context, Intent intent) {
        if (Intent.ACTION_BOOT_COMPLETED.equals(intent.getAction())) {
            SharedPreferences sp = context.getSharedPreferences("skl_police_state", Context.MODE_PRIVATE);
            String unitId = sp.getString("unitId", "");
            if (!unitId.isEmpty()) {
                Intent serviceIntent = new Intent(context, LocationTrackingService.class);
                serviceIntent.setAction(LocationTrackingService.ACTION_START);
                ContextCompat.startForegroundService(context, serviceIntent);
            }
        }
    }
}
