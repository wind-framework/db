<?php

namespace Wind\Db\Modifier;

/**
 * Set datetime when create or update model
 * @mixin \Wind\Db\Model
 */
trait DatetimeModifier
{

    /**
     * Event to fields map
     *
     * eg:
     * [
     *    Model::EVENT_BEFORE_CREATE => ['created_at', 'updated_at']
     * ]
     */
    // protected $datetimeAttributes = [];
    // protected $datetimeFormat = 'Y-m-d H:i:s';

    protected static function bootDatetimeModifier()
    {
        $datetimes = static::property('datetimeAttributes');

        if ($datetimes) {
            $format = static::property('datetimeFormat') ?: 'Y-m-d H:i:s';
            foreach ($datetimes as $event => $attributes) {
                self::on($event, static function($model) use ($attributes, $format) {
                    foreach ($attributes as $name) {
                        $model->$name = date($format);
                    }
                });
            }
        }
    }

}
