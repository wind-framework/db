<?php

namespace Wind\Db\Modifier;

/**
 * Set timestamp when create or update model
 * @mixin \Wind\Db\Model
 */
trait TimestampModifier
{

    /**
     * Event to fields map
     *
     * eg:
     * [
     *    Model::EVENT_BEFORE_CREATE => ['created_at', 'updated_at']
     * ]
     */
    // protected $timestampAttributes = [];

    protected static function bootTimestampModifier()
    {
        $timestamps = static::property('timestampAttributes');

        if ($timestamps) {
            foreach ($timestamps as $event => $attributes) {
                self::on($event, static function($model) use ($attributes) {
                    foreach ($attributes as $name) {
                        $model->$name = time();
                    }
                });
            }
        }
    }

}
